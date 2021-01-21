<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use ErrorException;
use Keboola\Csv\CsvOptions;
use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\DbExtractor\Configuration\ValueObject\SnowflakeDatabaseConfig;
use Keboola\DbExtractor\Configuration\ValueObject\SnowflakeMetadataProvider;
use Keboola\DbExtractor\DbRetryProxy;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Traits\QuoteTrait;
use Keboola\DbExtractor\Utils\AccountUrlParser;
use Keboola\Datatype\Definition\Snowflake as SnowflakeDatatype;
use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\InputTable;
use Keboola\SnowflakeDbAdapter\Connection;
use Keboola\SnowflakeDbAdapter\Exception\CannotAccessObjectException;
use Keboola\SnowflakeDbAdapter\QueryBuilder;
use Keboola\Temp\Temp;
use PDOException;
use Psr\Log\LoggerInterface;
use Symfony\Component\Process\Process;
use \SplFileInfo;
use \Exception;
use Throwable;
use Nette\Utils;

class Snowflake extends BaseExtractor
{
    use QuoteTrait;

    public const INCREMENT_TYPE_NUMERIC = 'numeric';
    public const INCREMENT_TYPE_TIMESTAMP = 'timestamp';
    public const INCREMENT_TYPE_DATE = 'date';
    public const NUMERIC_BASE_TYPES = ['INTEGER', 'NUMERIC', 'FLOAT'];
    public const SEMI_STRUCTURED_TYPES = ['VARIANT' , 'OBJECT', 'ARRAY'];

    /** @var Connection $db */
    protected $db;

    private SplFileInfo $snowSqlConfig;

    private ?string $warehouse = null;

    private string $database;

    private ?string $schema = null;

    private string $user;

    private Temp $temp;

    private string $incrementalFetchingColType;

    public function __construct(array $parameters, array $state, LoggerInterface $logger)
    {
        $this->temp = new Temp('ex-snowflake');

        parent::__construct($parameters, $state, $logger);
    }

    public function getMetadataProvider(): MetadataProvider
    {
        return new SnowflakeMetadataProvider($this->db, $this->database, $this->schema);
    }

    public function createConnection(DatabaseConfig $databaseConfig): Connection
    {
        if (!($databaseConfig instanceof SnowflakeDatabaseConfig)) {
            throw new ApplicationException('Instance of SnowflakeDatabaseConfig exceded');
        }
        $this->snowSqlConfig = $this->createSnowSqlConfig($databaseConfig);

        $databaseConfigArray = [
            'host' => $databaseConfig->getHost(),
            'user' => $databaseConfig->getUsername(),
            'password' => $databaseConfig->getPassword(),
            'port' => $databaseConfig->getPort(),
            'database' => $databaseConfig->getDatabase(),
        ];
        if (getenv('KBC_RUNID')) {
            $databaseConfigArray['runId'] = getenv('KBC_RUNID');
        }

        $this->user = $databaseConfig->getUsername();
        $this->database = $databaseConfig->getDatabase();

        if ($databaseConfig->hasWarehouse()) {
            $this->warehouse = $databaseConfig->getWarehouse();
            $databaseConfigArray['warehouse'] = $databaseConfig->getWarehouse();
        }

        try {
            $connection = new Connection($databaseConfigArray);
            if ($databaseConfig->hasSchema()) {
                $this->schema = $databaseConfig->getSchema();
                $connection->query(
                    sprintf(
                        'USE SCHEMA %s',
                        QueryBuilder::quoteIdentifier($databaseConfig->getSchema())
                    )
                );
            }
        } catch (CannotAccessObjectException $e) {
            throw new UserException($e->getMessage(), 0, $e);
        }

        return $connection;
    }

    public function testConnection(): void
    {
        $this->db->query('SELECT current_date;');

        $defaultWarehouse = $this->getUserDefaultWarehouse();
        if (!$defaultWarehouse && !$this->warehouse) {
            throw new UserException('Specify "warehouse" parameter');
        }

        $warehouse = (string) $defaultWarehouse;
        if ($this->warehouse) {
            $warehouse = (string) $this->warehouse;
        }

        try {
            $this->db->query(sprintf(
                'USE WAREHOUSE %s;',
                QueryBuilder::quoteIdentifier($warehouse)
            ));
        } catch (Throwable $e) {
            if (preg_match('/Object does not exist/ui', $e->getMessage())) {
                throw new UserException(sprintf('Invalid warehouse "%s" specified', $warehouse));
            } else {
                throw $e;
            }
        }
    }

    public function export(ExportConfig $exportConfig): array
    {
        $this->logger->info('Exporting to ' . $exportConfig->getOutputTable());
        if ($exportConfig->isIncrementalFetching()) {
            $this->validateIncrementalFetching($exportConfig);
            $maxValue = $this->canFetchMaxIncrementalValueSeparately($exportConfig) ?
                $this->getMaxOfIncrementalFetchingColumn($exportConfig) : null;
        } else {
            $maxValue = null;
        }

        $output = [
            'outputTable' => $exportConfig->getOutputTable(),
            'rows' => $this->exportAndDownload($exportConfig),
        ];

        // output state
        if ($maxValue) {
            $output['state']['lastFetchedRow'] = $maxValue;
        }

        return $output;
    }

    public function getMaxOfIncrementalFetchingColumn(ExportConfig $exportConfig): ?string
    {

        if ($exportConfig->hasIncrementalFetchingLimit()) {
            $fullsql = sprintf(
                'SELECT %s FROM %s.%s',
                QueryBuilder::quoteIdentifier($exportConfig->getIncrementalFetchingColumn()),
                QueryBuilder::quoteIdentifier($exportConfig->getTable()->getSchema()),
                QueryBuilder::quoteIdentifier($exportConfig->getTable()->getName())
            );

            $fullsql .= $this->createIncrementalAddon($exportConfig);

            $fullsql .= sprintf(
                ' LIMIT %s OFFSET %s',
                1,
                $exportConfig->getIncrementalFetchingLimit() - 1
            );
        } else {
            $fullsql = sprintf(
                'SELECT MAX(%s) as %s FROM %s.%s',
                QueryBuilder::quoteIdentifier($exportConfig->getIncrementalFetchingColumn()),
                QueryBuilder::quoteIdentifier($exportConfig->getIncrementalFetchingColumn()),
                QueryBuilder::quoteIdentifier($exportConfig->getTable()->getSchema()),
                QueryBuilder::quoteIdentifier($exportConfig->getTable()->getName())
            );
        }
        $result = $this->runRetriableQuery($fullsql, 'Error fetching maximum value');
        if (count($result) > 0) {
            return $result[0][$exportConfig->getIncrementalFetchingColumn()];
        }
        return null;
    }

    private function getColumnInfo(string $query): array
    {
        // Create temporary view from the supplied query
        $sql = sprintf(
            'SELECT * FROM (%s) LIMIT 0;',
            rtrim(trim($query), ';')
        );

        $this->runRetriableQuery(
            $sql,
            sprintf('DB query "%s" failed: ', rtrim(trim($query), ';'))
        );

        return $this->runRetriableQuery(
            'DESC RESULT LAST_QUERY_ID()',
            'Error fetching last query id'
        );
    }

    private function exportAndDownload(ExportConfig $exportConfig): int
    {
        if (!$exportConfig->hasQuery()) {
            $query = $this->simpleQuery($exportConfig);
            $columnInfo = $this->getColumnInfo($query);
            $objectColumns = array_filter($columnInfo, function ($column): bool {
                return in_array($column['type'], self::SEMI_STRUCTURED_TYPES);
            });
            if (!empty($objectColumns)) {
                $query = $this->simpleQueryWithCasting($exportConfig->getTable(), $columnInfo);
            }
        } else {
            $query = $exportConfig->getQuery();
            $columnInfo = $this->getColumnInfo($query);
        }

        $this->cleanupTableStage($exportConfig->getOutputTable());

        // copy into internal staging
        $copyCommand = $this->generateCopyCommand($exportConfig->getOutputTable(), $query);

        $res = $this->runRetriableQuery(
            $copyCommand,
            sprintf('Copy Command: %s failed with message', $copyCommand)
        );
        $rowCount = (int) ($res[0]['rows_unloaded'] ?? 0);

        if ($rowCount === 0) {
            // query resulted in no rows, nothing left to do
            return 0;
        }

        $this->logger->info('Downloading data from Snowflake');

        $outputDataDir = $this->dataDir . '/out/tables/' . $exportConfig->getOutputTable() . '.csv.gz';

        @mkdir($outputDataDir, 0755, true);

        $sql = [];
        if ($this->warehouse) {
            $sql[] = sprintf('USE WAREHOUSE %s;', QueryBuilder::quoteIdentifier($this->warehouse));
        }

        $sql[] = sprintf('USE DATABASE %s;', QueryBuilder::quoteIdentifier($this->database));

        if ($this->schema) {
            $sql[] = sprintf(
                'USE SCHEMA %s.%s;',
                QueryBuilder::quoteIdentifier($this->database),
                QueryBuilder::quoteIdentifier($this->schema)
            );
        }

        $sql[] = sprintf(
            'GET @~/%s file://%s;',
            $exportConfig->getOutputTable(),
            $outputDataDir
        );

        $snowSql = $this->temp->createTmpFile('snowsql.sql');
        file_put_contents($snowSql->getPathname(), implode("\n", $sql));

        $this->logger->debug(trim(implode("\n", $sql)));

        // execute external
        $command = sprintf(
            'snowsql --noup --config %s -c downloader -f %s',
            $this->snowSqlConfig,
            $snowSql
        );

        $this->logger->debug(trim($command));

        $process = Process::fromShellCommandline($command);
        $process->setTimeout(null);
        $process->run();

        if (!$process->isSuccessful()) {
            $this->logger->error(sprintf('Snowsql error, process output %s', $process->getOutput()));
            $this->logger->error(sprintf('Snowsql error: %s', $process->getErrorOutput()));
            throw new Exception(sprintf(
                'File download error occurred processing [%s]',
                $exportConfig->hasTable() ? $exportConfig->getTable()->getName() : $exportConfig->getOutputTable()
            ));
        }

        $csvFiles = $this->parseFiles($process->getOutput(), $outputDataDir);
        $bytesDownloaded = 0;
        foreach ($csvFiles as $csvFile) {
            $bytesDownloaded += $csvFile->getSize();
        }

        $this->logger->info(sprintf(
            '%d files (%s) downloaded',
            count($csvFiles),
            $this->dataSizeFormatted((int) $bytesDownloaded)
        ));

        $this->cleanupTableStage($exportConfig->getOutputTable());

        if ($rowCount > 0) {
            $this->createSnowflakeManifest($exportConfig, $columnInfo);
        } else {
            @unlink($this->getOutputFilename($exportConfig->getOutputTable())); // no rows, no file
            $this->logger->warning(sprintf(
                'Query returned empty result. Nothing was imported to [%s]',
                $exportConfig->getOutputTable()
            ));
        }

        return $rowCount;
    }

    private function generateCopyCommand(string $stageTmpPath, string $query): string
    {
        $csvOptions = [];
        $csvOptions[] = sprintf('FIELD_DELIMITER = %s', $this->quote(CsvOptions::DEFAULT_DELIMITER));
        $csvOptions[] = sprintf('FIELD_OPTIONALLY_ENCLOSED_BY = %s', $this->quote(CsvOptions::DEFAULT_ENCLOSURE));
        $csvOptions[] = sprintf('ESCAPE_UNENCLOSED_FIELD = %s', $this->quote('\\'));
        $csvOptions[] = sprintf('COMPRESSION = %s', $this->quote('GZIP'));
        $csvOptions[] = sprintf('NULL_IF=()');

        return sprintf(
            '
            COPY INTO @~/%s/part
            FROM (%s)

            FILE_FORMAT = (TYPE=CSV %s)
            HEADER = false
            MAX_FILE_SIZE=50000000
            OVERWRITE = TRUE
            ;
            ',
            $stageTmpPath,
            rtrim(trim($query), ';'),
            implode(' ', $csvOptions)
        );
    }

    private function dataSizeFormatted(int $bytes): string
    {
        $base = log($bytes) / log(1024);
        $suffixes = [' B', ' KB', ' MB', ' GB', ' TB'];
        return round(pow(1024, $base - floor($base)), 2) . $suffixes[(int) floor($base)];
    }

    public function validateIncrementalFetching(ExportConfig $exportConfig): void
    {
        $columns = $this->runRetriableQuery(
            sprintf(
                'SELECT * FROM INFORMATION_SCHEMA.COLUMNS as cols 
                            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s',
                $this->quote($exportConfig->getTable()->getSchema()),
                $this->quote($exportConfig->getTable()->getName()),
                $this->quote($exportConfig->getIncrementalFetchingColumn())
            ),
            'Error get column info'
        );
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
                $this->incrementalFetchingColType = self::INCREMENT_TYPE_NUMERIC;
            } else if ($datatype->getBasetype() === 'TIMESTAMP') {
                $this->incrementalFetchingColType = self::INCREMENT_TYPE_TIMESTAMP;
            } else if ($datatype->getBasetype() === 'DATE') {
                $this->incrementalFetchingColType = self::INCREMENT_TYPE_DATE;
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

    public function simpleQuery(ExportConfig $exportConfig): string
    {
        $incrementalAddon = null;
        if ($exportConfig->isIncrementalFetching()) {
            $incrementalAddon = $this->createIncrementalAddon($exportConfig);
        }
        if ($exportConfig->hasColumns()) {
            $query = sprintf(
                'SELECT %s FROM %s.%s',
                implode(', ', array_map(function ($column): string {
                    return QueryBuilder::quoteIdentifier($column);
                }, $exportConfig->getColumns())),
                QueryBuilder::quoteIdentifier($exportConfig->getTable()->getSchema()),
                QueryBuilder::quoteIdentifier($exportConfig->getTable()->getName())
            );
        } else {
            $query = sprintf(
                'SELECT * FROM %s.%s',
                QueryBuilder::quoteIdentifier($exportConfig->getTable()->getSchema()),
                QueryBuilder::quoteIdentifier($exportConfig->getTable()->getName())
            );
        }
        if ($incrementalAddon) {
            $query .= $incrementalAddon;
        }
        if ($exportConfig->hasIncrementalFetchingLimit()) {
            $query .= sprintf(
                ' LIMIT %d',
                $exportConfig->getIncrementalFetchingLimit()
            );
        }
        return $query;
    }

    private function createIncrementalAddon(ExportConfig $exportConfig): string
    {
        $incrementalAddon = '';
        if (isset($this->state['lastFetchedRow'])) {
            if ($this->incrementalFetchingColType === self::INCREMENT_TYPE_NUMERIC) {
                $lastFetchedRow = $this->state['lastFetchedRow'];
            } else {
                $lastFetchedRow = $this->quote((string) $this->state['lastFetchedRow']);
            }
            $incrementalAddon = sprintf(
                ' WHERE %s >= %s',
                QueryBuilder::quoteIdentifier($exportConfig->getIncrementalFetchingColumn()),
                $lastFetchedRow
            );
        }
        $incrementalAddon .= sprintf(
            ' ORDER BY %s',
            QueryBuilder::quoteIdentifier($exportConfig->getIncrementalFetchingColumn())
        );
        return $incrementalAddon;
    }

    private function simpleQueryWithCasting(InputTable $table, array $columnInfo): string
    {
        return sprintf(
            'SELECT %s FROM %s.%s',
            implode(', ', array_map(function ($column): string {
                if (in_array($column['type'], self::SEMI_STRUCTURED_TYPES)) {
                    return sprintf(
                        'CAST(%s AS TEXT) AS %s',
                        QueryBuilder::quoteIdentifier($column['name']),
                        QueryBuilder::quoteIdentifier($column['name'])
                    );
                }
                return QueryBuilder::quoteIdentifier($column['name']);
            }, $columnInfo)),
            QueryBuilder::quoteIdentifier($table->getSchema()),
            QueryBuilder::quoteIdentifier($table->getName())
        );
    }

    private function parseFiles(string $output, string $path): array
    {
        $files = [];
        $lines = explode("\n", $output);

        $lines = array_map(
            function ($item): array {
                $item = trim($item, '|');
                return array_map('trim', explode('|', $item));
            },
            array_filter(
                $lines,
                function ($item): bool {
                    $item = trim($item);
                    return preg_match('/^\|.+\|$/ui', $item) && preg_match('/([\.a-z0-9_\-]+\.gz)/ui', $item);
                }
            )
        );

        foreach ($lines as $line) {
            if (!preg_match('/^downloaded$/ui', $line[2])) {
                throw new Exception(sprintf(
                    'Cannot download file: %s Status: %s Size: %s Message: %s',
                    $line[0],
                    $line[2],
                    $line[1],
                    $line[3]
                ));
            }

            $file = new SplFileInfo($path . '/' . $line[0]);
            if ($file->isFile()) {
                $files[] = $file;
            } else {
                throw new Exception('Missing file: ' . $line[0]);
            }
        }

        return $files;
    }

    private function quote(string $value): string
    {
        return "'" . addslashes($value) . "'";
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

    private function getUserDefaultWarehouse(): ?string
    {
        $sql = sprintf(
            'DESC USER %s;',
            QueryBuilder::quoteIdentifier($this->user)
        );

        $config = $this->runRetriableQuery($sql, 'Error get user config');

        foreach ($config as $item) {
            if ($item['property'] === 'DEFAULT_WAREHOUSE') {
                return $item['value'] === 'null' ? null : $item['value'];
            }
        }

        return null;
    }

    private function runRetriableQuery(string $query, string $errorMessage = '', string $type = 'fetchAll'): array
    {
        $retryProxy = new DbRetryProxy(
            $this->logger,
            DbRetryProxy::DEFAULT_MAX_TRIES,
            [PDOException::class, ErrorException::class, Throwable::class]
        );
        return $retryProxy->call(function () use ($query, $type, $errorMessage): array {
            try {
                return (array) $this->db->{$type}($query);
            } catch (Throwable $e) {
                throw new UserException(
                    $errorMessage . ': ' . $e->getMessage(),
                    0,
                    $e
                );
            }
        });
    }

    private function cleanupTableStage(string $tmpTableName): void
    {
        $sql = sprintf('REMOVE @~/%s;', $tmpTableName);
        $this->runRetriableQuery($sql, 'Query execution error', 'query');
    }

    protected function createDatabaseConfig(array $data): DatabaseConfig
    {
        return SnowflakeDatabaseConfig::fromArray($data);
    }

    protected function createSnowflakeManifest(ExportConfig $exportConfig, array $columns): void
    {
        $metadataSerializer = $this->getManifestMetadataSerializer();
        $outFilename = $this->getOutputFilename($exportConfig->getOutputTable()) . '.gz.manifest';

        $manifestData = [
            'destination' => $exportConfig->getOutputTable(),
            'incremental' => $exportConfig->isIncrementalLoading(),
            'columns' => array_map(fn($v) => $v['name'], $columns),
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
            if (!empty($sanitizedPks)) {
                $manifestData['primary_key'] = $sanitizedPks;
            }
        }

        file_put_contents($outFilename, json_encode($manifestData));
    }

    protected function canFetchMaxIncrementalValueSeparately(ExportConfig $exportConfig): bool
    {
        return
            !$exportConfig->hasQuery() &&
            $exportConfig->isIncrementalFetching();
    }

    protected function getOutputFilename(string $outputTableName): string
    {
        $sanitizedTablename = Utils\Strings::webalize($outputTableName, '._', false);
        return $this->dataDir . '/out/tables/' . $sanitizedTablename . '.csv';
    }
}
