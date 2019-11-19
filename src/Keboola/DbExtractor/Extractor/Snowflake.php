<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Csv\CsvFile;
use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractorLogger\Logger;
use Keboola\Db\Import\Snowflake\Connection;
use Keboola\DbExtractor\Utils\AccountUrlParser;
use Keboola\Datatype\Definition\GenericStorage as GenericDatatype;
use Keboola\Datatype\Definition\Snowflake as SnowflakeDatatype;
use Keboola\Datatype\Definition\Exception\InvalidTypeException;
use Keboola\Temp\Temp;
use Retry\BackOff\ExponentialBackOffPolicy;
use Retry\Policy\SimpleRetryPolicy;
use Retry\RetryProxy;
use Symfony\Component\Process\Process;

class Snowflake extends Extractor
{
    public const INCREMENT_TYPE_NUMERIC = 'numeric';
    public const INCREMENT_TYPE_TIMESTAMP = 'timestamp';
    public const NUMERIC_BASE_TYPES = ['INTEGER', 'NUMERIC', 'FLOAT'];
    public const SEMI_STRUCTURED_TYPES = ['VARIANT' , 'OBJECT', 'ARRAY'];

    /** @var Connection */
    protected $db;

    /** @var \SplFileInfo */
    private $snowSqlConfig;

    /** @var string */
    private $warehouse;

    /** @var string */
    private $database;

    /** @var string */
    private $schema;

    /** @var string */
    private $user;

    /** @var Temp */
    private $temp;

    public function __construct(array $parameters, array $state, Logger $logger)
    {
        $this->temp = new Temp('ex-snowflake');

        parent::__construct($parameters, $state, $logger);
    }

    public function createConnection(array $dbParams): Connection
    {
        $dbParams['password'] = $dbParams['#password'];
        $this->snowSqlConfig = $this->createSnowSqlConfig($dbParams);

        $connection = new Connection($dbParams);

        $this->user = $dbParams['user'];

        $this->database = $dbParams['database'];

        if (!empty($dbParams['warehouse'])) {
            $this->warehouse = $dbParams['warehouse'];
        }

        if (!empty($dbParams['schema'])) {
            $this->schema = $dbParams['schema'];
            $connection->query(sprintf('USE SCHEMA %s', $connection->quoteIdentifier($this->schema)));
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
                $this->db->quoteIdentifier($warehouse)
            ));
        } catch (\Throwable $e) {
            if (preg_match('/Object does not exist/ui', $e->getMessage())) {
                throw new UserException(sprintf('Invalid warehouse "%s" specified', $warehouse));
            } else {
                throw $e;
            }
        }
    }

    public function export(array $table): array
    {
        $outputTable = $table['outputTable'];

        $this->logger->info('Exporting to ' . $outputTable);
        $maxValue = null;
        if (isset($table['table']) && isset($this->incrementalFetching)) {
            $maxValue = $this->getMaxOfIncrementalFetchingColumn($table['table']);
        }
        $rowCount = $this->exportAndDownload($table);

        $output = [
            'outputTable' => $outputTable,
            'rows' => $rowCount,
        ];
        // output state
        if ($maxValue) {
            $output['state']['lastFetchedRow'] = $maxValue;
        }

        return $output;
    }

    public function getMaxOfIncrementalFetchingColumn(array $table): ?string
    {

        if (isset($this->incrementalFetching['limit']) && $this->incrementalFetching['limit'] > 0) {
            $fullsql = sprintf(
                'SELECT %s FROM %s.%s',
                $this->db->quoteIdentifier($this->incrementalFetching['column']),
                $this->db->quoteIdentifier($table['schema']),
                $this->db->quoteIdentifier($table['tableName'])
            );

            $fullsql .= $this->createIncrementalAddon();

            $fullsql .= sprintf(
                ' LIMIT %s OFFSET %s',
                $this->incrementalFetching['limit'],
                $this->incrementalFetching['limit'] - 1
            );
        } else {
            $fullsql = sprintf(
                'SELECT MAX(%s) as %s FROM %s.%s',
                $this->db->quoteIdentifier($this->incrementalFetching['column']),
                $this->db->quoteIdentifier($this->incrementalFetching['column']),
                $this->db->quoteIdentifier($table['schema']),
                $this->db->quoteIdentifier($table['tableName'])
            );
        }
        $result = $this->db->fetchAll($fullsql);
        if (count($result) > 0) {
            return $result[0][$this->incrementalFetching['column']];
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

        try {
            $this->db->query($sql);
        } catch (\Throwable $e) {
            throw new UserException(
                sprintf('DB query "%s" failed: %s', rtrim(trim($query), ';'), $e->getMessage()),
                0,
                $e
            );
        }

        return $this->db->fetchAll('DESC RESULT LAST_QUERY_ID()');
    }

    private function exportAndDownload(array $table): int
    {
        if (!isset($table['query']) || $table['query'] === '') {
            $query = $this->simpleQuery($table['table'], $table['columns']);
            $columnInfo = $this->getColumnInfo($query);
            $objectColumns = array_filter($columnInfo, function ($column): bool {
                return in_array($column['type'], self::SEMI_STRUCTURED_TYPES);
            });
            if (!empty($objectColumns)) {
                $query = $this->simpleQueryWithCasting($table['table'], $columnInfo);
            }
        } else {
            $query = $table['query'];
            $columnInfo = $this->getColumnInfo($query);
        }

        $tmpTableName = str_replace('.', '_', $table['outputTable']);
        $this->cleanupTableStage($tmpTableName);

        // copy into internal staging
        $copyCommand = $this->generateCopyCommand($tmpTableName, $query);

        try {
            $res = $this->executeCopyCommand($copyCommand);
        } catch (\Throwable $e) {
            throw new UserException(
                sprintf('Copy Command: %s failed with message: %s', $copyCommand, $e->getMessage())
            );
        }

        if (count($res) > 0 && (int) $res[0]['rows_unloaded'] === 0) {
            // query resulted in no rows, nothing left to do
            return 0;
        }

        $rowCount = (int) $res[0]['rows_unloaded'];

        $this->logger->info('Downloading data from Snowflake');

        $outputDataDir = $this->dataDir . '/out/tables/' . $tmpTableName . '.csv.gz';

        @mkdir($outputDataDir, 0755, true);

        $sql = [];
        $sql[] = sprintf('USE DATABASE %s;', $this->db->quoteIdentifier($this->database));

        if ($this->schema) {
            $sql[] = sprintf('USE SCHEMA %s;', $this->db->quoteIdentifier($this->schema));
        }

        if ($this->warehouse) {
            $sql[] = sprintf('USE WAREHOUSE %s;', $this->db->quoteIdentifier($this->warehouse));
        }

        $sql[] = sprintf(
            'GET @~/%s file://%s;',
            $tmpTableName,
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
            throw new \Exception(sprintf('File download error occurred processing [%s]', $table['name']));
        }

        $csvFiles = $this->parseFiles($process->getOutput(), $outputDataDir);
        $bytesDownloaded = 0;
        foreach ($csvFiles as $csvFile) {
            $bytesDownloaded += $csvFile->getSize();
        }

        file_put_contents(
            $outputDataDir . '.manifest',
            json_encode(
                $this->createTableManifest(
                    $table,
                    array_map(
                        function ($column): string {
                            return $column['name'];
                        },
                        $columnInfo
                    )
                )
            )
        );

        $this->logger->info(sprintf(
            '%d files (%s) downloaded',
            count($csvFiles),
            $this->dataSizeFormatted((int) $bytesDownloaded)
        ));

        $this->cleanupTableStage($tmpTableName);

        return $rowCount;
    }

    private function generateCopyCommand(string $stageTmpPath, string $query): string
    {
        $csvOptions = [];
        $csvOptions[] = sprintf('FIELD_DELIMITER = %s', $this->quote(CsvFile::DEFAULT_DELIMITER));
        $csvOptions[] = sprintf('FIELD_OPTIONALLY_ENCLOSED_BY = %s', $this->quote(CsvFile::DEFAULT_ENCLOSURE));
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

    private function executeCopyCommand(string $copyCommand, int $maxTries = 5): array
    {
        $retryPolicy = new SimpleRetryPolicy($maxTries, ['PDOException', 'ErrorException', 'Exception']);
        $backOffPolicy = new ExponentialBackOffPolicy(1000);
        $proxy = new RetryProxy($retryPolicy, $backOffPolicy);
        $counter = 0;
        /** @var \Exception $lastException */
        $lastException = null;
        try {
            $ret = $proxy->call(function () use ($copyCommand, &$counter, &$lastException) {
                if ($counter > 0) {
                    $this->logger->info(sprintf('%s. Retrying... [%dx]', $lastException->getMessage(), $counter));
                }
                try {
                    return $this->db->fetchAll($copyCommand);
                } catch (\Throwable $e) {
                    $lastException = new UserException(
                        sprintf('Copy Command failed: %s', $e->getMessage()),
                        0,
                        $e
                    );
                    $counter++;
                    throw $e;
                }
            });
        } catch (\Throwable $e) {
            if ($lastException !== null) {
                throw $lastException;
            }
            throw $e;
        }
        return $ret;
    }

    private function createTableManifest(array $table, array $columns): array
    {
        $manifestData = [
            'destination' => $table['outputTable'],
            'delimiter' => CsvFile::DEFAULT_DELIMITER,
            'enclosure' => CsvFile::DEFAULT_ENCLOSURE,
            'primary_key' => $table['primaryKey'],
            'incremental' => $table['incremental'],
            'columns' => $columns,
        ];

        if (isset($table['table']) && isset($table['table']['tableName'])) {
            $tables = $this->getTables([$table['table']]);
            if (count($tables) > 0) {
                $tableDetails = $tables[0];
                $columnMetadata = [];
                foreach ($tableDetails['columns'] as $column) {
                    if (count($table['columns']) > 0 && !in_array($column['name'], $table['columns'])) {
                        continue;
                    }
                    $datatypeKeys = ['length', 'nullable', 'default'];
                    try {
                        $datatype = new SnowflakeDatatype(
                            $column['type'],
                            array_intersect_key($column, array_flip($datatypeKeys))
                        );
                    } catch (InvalidTypeException $e) {
                        if (!in_array($column['type'], self::SEMI_STRUCTURED_TYPES)) {
                            $this->logger->warning(
                                'Encountered irregular type: ' . $column['type'] . ' for culumn ' . $column['name']
                            );
                        }
                        $datatype = new GenericDatatype(
                            $column['type'],
                            array_intersect_key($column, array_flip($datatypeKeys))
                        );
                    }

                    $columnMetadata[$column['name']] = $datatype->toMetadata();
                    $nonDatatypeKeys = array_diff_key($column, array_flip($datatypeKeys));
                    foreach ($nonDatatypeKeys as $key => $value) {
                        if ($key !== 'name') {
                            $columnMetadata[$column['name']][] = [
                                'key' => 'KBC.' . $key,
                                'value'=> $value,
                            ];
                        }
                    }
                }
                unset($tableDetails['columns']);
                foreach ($tableDetails as $key => $value) {
                    $manifestData['metadata'][] = [
                        'key' => 'KBC.' . $key,
                        'value' => $value,
                    ];
                }
                $manifestData['column_metadata'] = $columnMetadata;
            }
        }

        return $manifestData;
    }

    private function dataSizeFormatted(int $bytes): string
    {
        $base = log($bytes) / log(1024);
        $suffixes = [' B', ' KB', ' MB', ' GB', ' TB'];
        return round(pow(1024, $base - floor($base)), 2) . $suffixes[(int) floor($base)];
    }

    public function getTables(?array $tables = null): array
    {
        $sql = $this->schema ? 'SHOW TABLES IN SCHEMA' : 'SHOW TABLES IN DATABASE';
        $arr = $this->db->fetchAll($sql);

        $sql = $this->schema ? 'SHOW VIEWS IN SCHEMA' : 'SHOW VIEWS IN DATABASE';
        $views = $this->db->fetchAll($sql);
        $arr = array_merge($arr, $views);

        $tableDefs = [];
        foreach ($arr as $table) {
            if ($this->shouldTableBeSkipped($table)) {
                continue;
            }
            if (is_null($tables) || !(array_search($table['name'], array_column($tables, 'tableName')) === false)) {
                $isView = array_key_exists('text', $table);
                $fullTableId = $table['schema_name'] . '.' . $table['name'];
                $tableDefs[$fullTableId] = [
                    'name' => $table['name'],
                    'catalog' => (isset($table['database_name'])) ? $table['database_name'] : null,
                    'schema' => (isset($table['schema_name'])) ? $table['schema_name'] : null,
                    'type' => $isView ? 'VIEW' : (isset($table['kind']) ? $table['kind'] : null),
                ];
                if (isset($table['rows'])) {
                    $tableDefs[$fullTableId]['rowCount'] = $table['rows'];
                }
                if (isset($table['bytes'])) {
                    $tableDefs[$fullTableId]['byteCount'] = $table['bytes'];
                }
            }
        }

        if (count($tableDefs) === 0) {
            return [];
        }

        $sqlWhereElements = [];
        foreach ($tableDefs as $fullTableId => $tableDef) {
            $sqlWhereElements[] = sprintf(
                '(table_schema = %s AND table_name = %s)',
                $this->quote($tableDef['schema']),
                $this->quote($tableDef['name'])
            );
        }
        $sqlWhereClause = sprintf('WHERE %s', implode(' OR ', $sqlWhereElements));

        $sql = 'SELECT * FROM information_schema.columns '
            . $sqlWhereClause
            . ' ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION';

        $columns = $this->db->fetchAll($sql);
        foreach ($columns as $i => $column) {
            $curTable = $column['TABLE_SCHEMA'] . '.' . $column['TABLE_NAME'];
            $length = ($column['CHARACTER_MAXIMUM_LENGTH']) ? $column['CHARACTER_MAXIMUM_LENGTH'] : null;
            if (is_null($length) && !is_null($column['NUMERIC_PRECISION'])) {
                if (is_numeric($column['NUMERIC_SCALE'])) {
                    $length = $column['NUMERIC_PRECISION'] . ',' . $column['NUMERIC_SCALE'];
                } else {
                    $length = $column['NUMERIC_PRECISION'];
                }
            }

            $curColumn = [
                'name' => $column['COLUMN_NAME'],
                'default' => $column['COLUMN_DEFAULT'],
                'length' => $length,
                'nullable' => (trim($column['IS_NULLABLE']) === 'NO') ? false : true,
                'type' => $column['DATA_TYPE'],
                'ordinalPosition' => $column['ORDINAL_POSITION'],
            ];

            if (!array_key_exists('columns', $tableDefs[$curTable])) {
                $tableDefs[$curTable]['columns'] = [];
            }
            $tableDefs[$curTable]['columns'][] = $curColumn;
        }
        return array_values($tableDefs);
    }

    private function shouldTableBeSkipped(array $table): bool
    {
        $isFromDifferentSchema = $this->schema && $table['schema_name'] !== $this->schema;
        $isFromInformationSchema = $table['schema_name'] === 'INFORMATION_SCHEMA';
        return $isFromDifferentSchema || $isFromInformationSchema;
    }

    public function validateIncrementalFetching(array $table, string $columnName, ?int $limit = null): void
    {
        $columns = $this->db->fetchAll(
            sprintf(
                'SELECT * FROM INFORMATION_SCHEMA.COLUMNS as cols 
                            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s',
                $this->quote($table['schema']),
                $this->quote($table['tableName']),
                $this->quote($columnName)
            )
        );
        if (count($columns) === 0) {
            throw new UserException(
                sprintf(
                    'Column [%s] specified for incremental fetching was not found in the table',
                    $columnName
                )
            );
        }

        try {
            $datatype = new SnowflakeDatatype($columns[0]['DATA_TYPE']);
            if (in_array($datatype->getBasetype(), self::NUMERIC_BASE_TYPES)) {
                $this->incrementalFetching['column'] = $columnName;
                $this->incrementalFetching['type'] = self::INCREMENT_TYPE_NUMERIC;
            } else if ($datatype->getBasetype() === 'TIMESTAMP') {
                $this->incrementalFetching['column'] = $columnName;
                $this->incrementalFetching['type'] = self::INCREMENT_TYPE_TIMESTAMP;
            } else {
                throw new UserException('invalid incremental fetching column type');
            }
        } catch (InvalidLengthException | UserException $exception) {
            throw new UserException(
                sprintf(
                    'Column [%s] specified for incremental fetching is not a numeric or timestamp type column',
                    $columnName
                )
            );
        }

        if ($limit) {
            $this->incrementalFetching['limit'] = $limit;
        }
    }

    public function simpleQuery(array $table, array $columns = array()): string
    {
        $incrementalAddon = null;
        if ($this->incrementalFetching && isset($this->incrementalFetching['column'])) {
            $incrementalAddon = $this->createIncrementalAddon();
        }
        if (count($columns) > 0) {
            $query = sprintf(
                'SELECT %s FROM %s.%s',
                implode(', ', array_map(function ($column): string {
                    return $this->db->quoteIdentifier($column);
                }, $columns)),
                $this->db->quoteIdentifier($table['schema']),
                $this->db->quoteIdentifier($table['tableName'])
            );
        } else {
            $query = sprintf(
                'SELECT * FROM %s.%s',
                $this->db->quoteIdentifier($table['schema']),
                $this->db->quoteIdentifier($table['tableName'])
            );
        }
        if ($incrementalAddon) {
            $query .= $incrementalAddon;
        }
        if (isset($this->incrementalFetching['limit'])) {
            $query .= sprintf(
                ' LIMIT %d',
                $this->incrementalFetching['limit']
            );
        }
        return $query;
    }

    private function createIncrementalAddon(): string
    {
        $incrementalAddon = '';
        if (isset($this->state['lastFetchedRow'])) {
            if ($this->incrementalFetching['type'] === self::INCREMENT_TYPE_NUMERIC) {
                $lastFetchedRow = $this->state['lastFetchedRow'];
            } else {
                $lastFetchedRow = $this->quote((string) $this->state['lastFetchedRow']);
            }
            $incrementalAddon = sprintf(
                ' WHERE %s >= %s',
                $this->db->quoteIdentifier($this->incrementalFetching['column']),
                $lastFetchedRow
            );
        }
        $incrementalAddon .= sprintf(
            ' ORDER BY %s',
            $this->db->quoteIdentifier($this->incrementalFetching['column'])
        );
        return $incrementalAddon;
    }

    private function simpleQueryWithCasting(array $table, array $columnInfo): string
    {
        return sprintf(
            'SELECT %s FROM %s.%s',
            implode(', ', array_map(function ($column): string {
                if (in_array($column['type'], self::SEMI_STRUCTURED_TYPES)) {
                    return sprintf(
                        'CAST(%s AS TEXT) AS %s',
                        $this->db->quoteIdentifier($column['name']),
                        $this->db->quoteIdentifier($column['name'])
                    );
                }
                return $this->db->quoteIdentifier($column['name']);
            }, $columnInfo)),
            $this->db->quoteIdentifier($table['schema']),
            $this->db->quoteIdentifier($table['tableName'])
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
                    return preg_match('/^\|.+\|$/ui', $item) && preg_match('/([a-z0-9\_\-\.]+\.gz)/ui', $item);
                }
            )
        );

        foreach ($lines as $line) {
            if (!preg_match('/^downloaded$/ui', $line[2])) {
                throw new \Exception(sprintf(
                    'Cannot download file: %s Status: %s Size: %s Message: %s',
                    $line[0],
                    $line[2],
                    $line[1],
                    $line[3]
                ));
            }

            $file = new \SplFileInfo($path . '/' . $line[0]);
            if ($file->isFile()) {
                $files[] = $file;
            } else {
                throw new \Exception('Missing file: ' . $line[0]);
            }
        }

        return $files;
    }

    private function quote(string $value): string
    {
        return "'" . addslashes($value) . "'";
    }

    private function createSnowSqlConfig(array $dbParams): \SplFileInfo
    {
        $cliConfig[] = '';
        $cliConfig[] = '[options]';
        $cliConfig[] = 'exit_on_error = true';
        $cliConfig[] = '';
        $cliConfig[] = '[connections.downloader]';
        $cliConfig[] = sprintf('accountname = "%s"', AccountUrlParser::parse($dbParams['host']));
        $cliConfig[] = sprintf('username = "%s"', $dbParams['user']);
        $cliConfig[] = sprintf('password = "%s"', $dbParams['#password']);
        $cliConfig[] = sprintf('dbname = "%s"', $dbParams['database']);

        if (isset($dbParams['warehouse'])) {
            $cliConfig[] = sprintf('warehousename = "%s"', $dbParams['warehouse']);
        }

        if (isset($dbParams['schema'])) {
            $cliConfig[] = sprintf('schemaname = "%s"', $dbParams['schema']);
        }

        $file = $this->temp->createFile('snowsql.config');
        file_put_contents($file->getPathname(), implode("\n", $cliConfig));

        return $file;
    }

    private function getUserDefaultWarehouse(): ?string
    {
        $sql = sprintf(
            'DESC USER %s;',
            $this->db->quoteIdentifier($this->user)
        );

        $config = $this->db->fetchAll($sql);

        foreach ($config as $item) {
            if ($item['property'] === 'DEFAULT_WAREHOUSE') {
                return $item['value'] === 'null' ? null : $item['value'];
            }
        }

        return null;
    }

    private function execQuery(string $query): void
    {
        try {
            $this->db->query($query);
        } catch (\Throwable $e) {
            throw new UserException('Query execution error: ' . $e->getMessage(), 0, $e);
        }
    }

    private function cleanupTableStage(string $tmpTableName): void
    {
        $sql = sprintf('REMOVE @~/%s;', $tmpTableName);
        $this->execQuery($sql);
    }
}
