<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\DbExtractor\Adapter\ExportAdapter;
use Keboola\DbExtractor\Adapter\ODBC\OdbcConnection;
use Keboola\DbExtractor\Configuration\ValueObject\SnowflakeDatabaseConfig;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Utils\AccountUrlParser;
use Keboola\Datatype\Definition\Snowflake as SnowflakeDatatype;
use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Keboola\SnowflakeDbAdapter\Exception\CannotAccessObjectException;
use Keboola\Temp\Temp;
use Psr\Log\LoggerInterface;
use \SplFileInfo;
use Throwable;
use Nette\Utils;

class Snowflake extends BaseExtractor
{
    public const INCREMENT_TYPE_NUMERIC = 'numeric';
    public const INCREMENT_TYPE_TIMESTAMP = 'timestamp';
    public const INCREMENT_TYPE_DATE = 'date';
    public const NUMERIC_BASE_TYPES = ['INTEGER', 'NUMERIC', 'FLOAT'];
    public const SEMI_STRUCTURED_TYPES = ['VARIANT' , 'OBJECT', 'ARRAY'];

    protected OdbcConnection $connection;

    private SplFileInfo $snowSqlConfig;

    private ?string $warehouse = null;

    private Temp $temp;

    private SnowflakeQueryFactory $queryFactory;

    public function __construct(array $parameters, array $state, LoggerInterface $logger)
    {
        $this->temp = new Temp('ex-snowflake');

        parent::__construct($parameters, $state, $logger);
    }

    public function getMetadataProvider(): SnowflakeMetadataProvider
    {
        return new SnowflakeMetadataProvider($this->connection, $this->getDatabaseConfig());
    }

    protected function createExportAdapter(): ExportAdapter
    {
        return new SnowsqlExportAdapter(
            $this->logger,
            $this->connection,
            $this->getQueryFactory(),
            $this->getDatabaseConfig(),
            $this->snowSqlConfig,
            $this->temp,
            $this->getMetadataProvider()
        );
    }

    protected function getQueryFactory(): SnowflakeQueryFactory
    {
        if (!isset($this->queryFactory)) {
            $this->queryFactory = new SnowflakeQueryFactory($this->state);
        }

        return $this->queryFactory;
    }

    public function createConnection(DatabaseConfig $databaseConfig): void
    {
        if (!($databaseConfig instanceof SnowflakeDatabaseConfig)) {
            throw new ApplicationException('Instance of SnowflakeDatabaseConfig exceded');
        }
        $this->snowSqlConfig = $this->createSnowSqlConfig($databaseConfig);

        if ($databaseConfig->hasWarehouse()) {
            $this->warehouse = $databaseConfig->getWarehouse();
        }

        try {
            $this->connection = new SnowflakeOdbcConnection(
                $this->logger,
                $databaseConfig
            );
            if ($databaseConfig->hasSchema()) {
                $this->connection->query(
                    sprintf(
                        'USE SCHEMA %s.%s',
                        $this->connection->quoteIdentifier($databaseConfig->getDatabase()),
                        $this->connection->quoteIdentifier($databaseConfig->getSchema())
                    )
                )->closeCursor();
                $this->logger->info(sprintf(
                    'Use schema "%s" in database "%s"',
                    $databaseConfig->getSchema(),
                    $databaseConfig->getDatabase()
                ));
            }
            if (getenv('KBC_RUNID')) {
                $queryTag = ['runId' => getenv('KBC_RUNID')];

                $this->connection->query("ALTER SESSION SET QUERY_TAG='" . json_encode($queryTag) . "';");
            }
        } catch (CannotAccessObjectException $e) {
            throw new UserException($e->getMessage(), 0, $e);
        }
    }

    public function testConnection(): void
    {
        $this->connection->query('SELECT current_date;');

        $defaultWarehouse = $this->getUserDefaultWarehouse();
        if (!$defaultWarehouse && !$this->warehouse) {
            throw new UserException('Specify "warehouse" parameter');
        }

        $warehouse = (string) $defaultWarehouse;
        if ($this->warehouse) {
            $warehouse = (string) $this->warehouse;
        }

        try {
            $this->connection->query(sprintf(
                'USE WAREHOUSE %s;',
                $this->connection->quoteIdentifier($warehouse)
            ));
        } catch (Throwable $e) {
            if (preg_match('/Object does not exist/ui', $e->getMessage())) {
                throw new UserException(sprintf('Invalid warehouse "%s" specified', $warehouse));
            } else {
                throw $e;
            }
        }
    }

    public function getMaxOfIncrementalFetchingColumn(ExportConfig $exportConfig): ?string
    {
        if ($exportConfig->hasIncrementalFetchingLimit()) {
            $fullsql = sprintf(
                'SELECT %s FROM %s.%s',
                $this->connection->quoteIdentifier($exportConfig->getIncrementalFetchingColumn()),
                $this->connection->quoteIdentifier($exportConfig->getTable()->getSchema()),
                $this->connection->quoteIdentifier($exportConfig->getTable()->getName())
            );

            $fullsql .= $this->getQueryFactory()->createIncrementalAddon($exportConfig, $this->connection);

            $fullsql .= sprintf(
                ' LIMIT %s OFFSET %s',
                1,
                $exportConfig->getIncrementalFetchingLimit() - 1
            );
        } else {
            $fullsql = sprintf(
                'SELECT MAX(%s) as %s FROM %s.%s',
                $this->connection->quoteIdentifier($exportConfig->getIncrementalFetchingColumn()),
                $this->connection->quoteIdentifier($exportConfig->getIncrementalFetchingColumn()),
                $this->connection->quoteIdentifier($exportConfig->getTable()->getSchema()),
                $this->connection->quoteIdentifier($exportConfig->getTable()->getName())
            );
        }
        $result = $this->connection->query($fullsql)->fetchAll();
        if (count($result) > 0) {
            return $result[0][$exportConfig->getIncrementalFetchingColumn()];
        }

        return $this->state['lastFetchedRow'] ?? null;
    }

    protected function validateIncrementalFetching(ExportConfig $exportConfig): void
    {
        $columns = $this->connection->query(
            sprintf(
                'SELECT * FROM INFORMATION_SCHEMA.COLUMNS as cols 
                            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s',
                $this->connection->quote($exportConfig->getTable()->getSchema()),
                $this->connection->quote($exportConfig->getTable()->getName()),
                $this->connection->quote($exportConfig->getIncrementalFetchingColumn())
            )
        )->fetchAll();

        if (count($columns) === 0) {
            throw new UserException(
                sprintf(
                    'Column [%s] specified for incremental fetching was not found in the table',
                    $exportConfig->getIncrementalFetchingColumn()
                )
            );
        }

        try {
            $datatype = new SnowflakeDatatype($columns[0]['DATA_TYPE']);
            if (in_array($datatype->getBasetype(), self::NUMERIC_BASE_TYPES)) {
                $this
                    ->getQueryFactory()
                    ->setIncrementalFetchingColType(self::INCREMENT_TYPE_NUMERIC)
                ;
            } else if ($datatype->getBasetype() === 'TIMESTAMP') {
                $this
                    ->getQueryFactory()
                    ->setIncrementalFetchingColType(self::INCREMENT_TYPE_TIMESTAMP)
                ;
            } else if ($datatype->getBasetype() === 'DATE') {
                $this
                    ->getQueryFactory()
                    ->setIncrementalFetchingColType(self::INCREMENT_TYPE_DATE)
                ;
            } else {
                throw new UserException('invalid incremental fetching column type');
            }
        } catch (InvalidLengthException | UserException $exception) {
            throw new UserException(
                sprintf(
                    'Column [%s] specified for incremental fetching is not a numeric, date or timestamp type column',
                    $exportConfig->getIncrementalFetchingColumn()
                )
            );
        }
    }

    protected function createDatabaseConfig(array $data): DatabaseConfig
    {
        return SnowflakeDatabaseConfig::fromArray($data);
    }

    private function getUserDefaultWarehouse(): ?string
    {
        $sql = sprintf(
            'DESC USER %s;',
            $this->connection->quoteIdentifier($this->getDatabaseConfig()->getUsername())
        );

        $config = $this->connection->query($sql)->fetchAll();

        foreach ($config as $item) {
            if ($item['property'] === 'DEFAULT_WAREHOUSE') {
                return $item['value'] === 'null' ? null : $item['value'];
            }
        }

        return null;
    }

    private function createSnowSqlConfig(SnowflakeDatabaseConfig $databaseConfig): SplFileInfo
    {
        $cliConfig[] = '';
        $cliConfig[] = '[options]';
        $cliConfig[] = 'exit_on_error = true';
        $cliConfig[] = '';
        $cliConfig[] = '[connections.downloader]';
        $cliConfig[] = sprintf('accountname = "%s"', AccountUrlParser::parse($databaseConfig->getHost()));
        $cliConfig[] = sprintf('username = "%s"', $databaseConfig->getUsername());
        $cliConfig[] = sprintf('password = "%s"', $databaseConfig->getPassword());
        $cliConfig[] = sprintf('dbname = "%s"', $databaseConfig->getDatabase());

        if ($databaseConfig->hasWarehouse()) {
            $cliConfig[] = sprintf('warehousename = "%s"', $databaseConfig->getWarehouse());
        }

        if ($databaseConfig->hasSchema()) {
            $cliConfig[] = sprintf('schemaname = "%s"', $databaseConfig->getSchema());
        }

        $file = $this->temp->createFile('snowsql.config');
        file_put_contents($file->getPathname(), implode("\n", $cliConfig));

        return $file;
    }

    protected function canFetchMaxIncrementalValueSeparately(ExportConfig $exportConfig): bool
    {
        return
            !$exportConfig->hasQuery() &&
            $exportConfig->isIncrementalFetching();
    }

    protected function getOutputFilename(string $outputTableName): string
    {
        $sanitizedTableName = Utils\Strings::webalize($outputTableName, '._');
        $outTablesDir = $this->dataDir . '/out/tables';
        return $outTablesDir . '/' . $sanitizedTableName . '.csv.gz';
    }

    protected function createManifest(ExportConfig $exportConfig): void
    {
        $metadataSerializer = $this->getManifestMetadataSerializer();
        $outFilename = $this->getOutputFilename($exportConfig->getOutputTable()) . '.manifest';

        $manifestData = [
            'destination' => $exportConfig->getOutputTable(),
            'incremental' => $exportConfig->isIncrementalLoading(),
        ];

        if ($exportConfig->hasPrimaryKey()) {
            $manifestData['primary_key'] = $exportConfig->getPrimaryKey();
        }

        if (!$exportConfig->hasQuery()) {
            $table = $this->getMetadataProvider()->getTable($exportConfig->getTable());
            $allTableColumns = $table->getColumns();
            $columnMetadata = [];
            $sanitizedPks = [];
            $exportedColumns = $exportConfig->hasColumns() ? $exportConfig->getColumns() : $allTableColumns->getNames();
            foreach ($exportedColumns as $index => $columnName) {
                $column = $allTableColumns->getByName($columnName);
                $columnMetadata[$column->getSanitizedName()] = $metadataSerializer->serializeColumn($column);

                // Sanitize PKs defined in the configuration
                if ($exportConfig->hasPrimaryKey() &&
                    in_array($column->getName(), $exportConfig->getPrimaryKey(), true)
                ) {
                    $sanitizedPks[] = $column->getSanitizedName();
                }
            }

            $manifestData['metadata'] = $metadataSerializer->serializeTable($table);
            $manifestData['column_metadata'] = $columnMetadata;
            $manifestData['columns'] = array_keys($columnMetadata);
            if (!empty($sanitizedPks)) {
                $manifestData['primary_key'] = $sanitizedPks;
            }
        } else {
            $columns = $this->getMetadataProvider()->getColumnInfo($exportConfig->getQuery());

            $manifestData['columns'] =  array_map(fn($v) => $v['name'], $columns);
        }

        file_put_contents($outFilename, json_encode($manifestData));
    }
}
