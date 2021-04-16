<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\DbExtractor\Adapter\ExportAdapter;
use Keboola\DbExtractor\Adapter\ODBC\OdbcConnection;
use Keboola\DbExtractor\Configuration\ValueObject\SnowflakeDatabaseConfig;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\Datatype\Definition\Snowflake as SnowflakeDatatype;
use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Nette\Utils;

class Snowflake extends BaseExtractor
{
    public const INCREMENT_TYPE_NUMERIC = 'numeric';
    public const INCREMENT_TYPE_TIMESTAMP = 'timestamp';
    public const INCREMENT_TYPE_DATE = 'date';
    public const NUMERIC_BASE_TYPES = ['INTEGER', 'NUMERIC', 'FLOAT'];
    public const SEMI_STRUCTURED_TYPES = ['VARIANT' , 'OBJECT', 'ARRAY'];

    protected OdbcConnection $connection;

    private SnowflakeQueryFactory $queryFactory;

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
        $factory = new SnowflakeConnectionFactory($this->logger);
        $this->connection = $factory->create($databaseConfig);
    }

    public function testConnection(): void
    {
        $this->connection->testConnection();
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
