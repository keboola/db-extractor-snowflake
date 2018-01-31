<?php
namespace Keboola\DbExtractor\Extractor;

use Keboola\Csv\CsvFile;
use Keboola\Db\Import\Exception;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Logger;
use Keboola\Db\Import\Snowflake\Connection;
use Keboola\DbExtractor\Utils\AccountUrlParser;
use Keboola\Datatype\Definition\Snowflake as SnowflakeDatatype;
use Keboola\Temp\Temp;
use Symfony\Component\Process\Process;
use Symfony\Component\Yaml\Yaml;

class Snowflake extends Extractor
{
    /**
     * @var Connection
     */
    protected $db;

    /**
     * @var \SplFileInfo
     */
    private $snowSqlConfig;

    private $warehouse;
    private $database;
    private $schema;
    private $user;

    /**
     * @var Temp
     */
    private $temp;

    public function __construct($parameters, Logger $logger)
    {
        $this->temp = new Temp('ex-snowflake');

        parent::__construct($parameters, $logger);
    }

    public function createConnection($dbParams)
    {
        $this->snowSqlConfig = $this->crateSnowSqlConfig($dbParams);

        $connection = new Connection($dbParams);

        $this->user = $dbParams['user'];

        $this->database = $dbParams['database'];
        $this->schema = $dbParams['schema'];

        if (!empty($dbParams['warehouse'])) {
            $this->warehouse = $dbParams['warehouse'];
        }

        $connection->query(sprintf("USE SCHEMA %s", $connection->quoteIdentifier($this->schema)));

        return $connection;
    }

    public function testConnection()
    {
        $this->db->query('SELECT current_date;');

        $defaultWarehouse = $this->getUserDefaultWarehouse();
        if (!$defaultWarehouse && !$this->warehouse) {
            throw new UserException('Specify "warehouse" parameter');
        }

        $warehouse = $defaultWarehouse;
        if ($this->warehouse) {
            $warehouse = $this->warehouse;
        }

        try {
            $this->db->query(sprintf(
                'USE WAREHOUSE %s;',
                $this->db->quoteIdentifier($warehouse)
            ));
        } catch (\Exception $e) {
            if (preg_match('/Object does not exist/ui', $e->getMessage())) {
                throw new UserException(sprintf('Invalid warehouse "%s" specified', $warehouse));
            } else {
                throw $e;
            }
        }
    }

    public function export(array $table)
    {
        $outputTable = $table['outputTable'];

        $this->logger->info("Exporting to " . $outputTable);


        $this->exportAndDownload($table);

        return $outputTable;
    }

    private function exportAndDownload(array $table)
    {
        if (!isset($table['query']) || $table['query'] === '') {
            $query = $this->simpleQuery($table['table'], $table['columns']);
        } else {
            $query = $table['query'];
        }

        $tmpTableName = str_replace('.', '_', $table['outputTable']);
        $sql = $this->cleanupTableStage($tmpTableName);

        // Create temporary view from the supplied query
        $sql = sprintf(
            "SELECT * FROM (%s) LIMIT 0;",
            rtrim(trim($query), ';')
        );

        try {
            $this->db->query($sql);
        } catch (\Exception $e) {
            throw new UserException(
                sprintf('DB query failed: %s', $e->getMessage()),
                0,
                $e
            );
        }

        $columns = array_map(
            function ($column) {
                return $column['name'];
            },
            $this->db->fetchAll("DESC RESULT LAST_QUERY_ID()")
        );


        // copy into internal staging
        $res = $this->db->fetchAll($this->generateCopyCommand($tmpTableName, $query));
        if (count($res) > 0 && (int) $res[0]['rows_unloaded'] === 0) {
            // query resulted in no rows, nothing left to do
            return;
        }

        $this->logger->info("Downloading data from Snowflake");

        $outputDataDir = $this->dataDir . '/out/tables/' . $tmpTableName . ".csv.gz";

        @mkdir($outputDataDir, 0755, true);

        $sql = [];
        $sql[] = sprintf('USE DATABASE %s;', $this->db->quoteIdentifier($this->database));
        $sql[] = sprintf('USE SCHEMA %s;', $this->db->quoteIdentifier($this->schema));

        if ($this->warehouse) {
            $sql[] = sprintf('USE WAREHOUSE %s;', $this->db->quoteIdentifier($this->warehouse));
        }

        $sql[] = sprintf(
            'GET @~/%s file://%s;',
            $tmpTableName,
            $outputDataDir
        );

        $snowSql = $this->temp->createTmpFile('snowsql.sql');
        file_put_contents($snowSql, implode("\n", $sql));

        $this->logger->debug(trim(implode("\n", $sql)));

        // execute external
        $command = sprintf(
            "snowsql --noup --config %s -c downloader -f %s",
            $this->snowSqlConfig,
            $snowSql
        );

        $this->logger->debug(trim($command));

        $process = new Process($command);
        $process->setTimeout(null);
        $process->run();

        if (!$process->isSuccessful()) {
            $this->logger->error(sprintf("Snowsql error, process output %s", $process->getOutput()));
            $this->logger->error(sprintf("Snowsql error: %s", $process->getErrorOutput()));
            throw new \Exception("File download error occurred");
        }

        $csvFiles = $this->parseFiles($process->getOutput(), $outputDataDir);
        $bytesDownloaded = 0;
        foreach ($csvFiles as $csvFile) {
            $bytesDownloaded += $csvFile->getSize();
        }

        file_put_contents(
            $outputDataDir . '.manifest',
            Yaml::dump($this->createTableManifest($table, $columns))
        );

        $this->logger->info(sprintf(
            "%d files (%s) downloaded",
            count($csvFiles),
            $this->dataSizeFormatted($bytesDownloaded)
        ));

        $this->cleanupTableStage($tmpTableName);
    }

    private function generateCopyCommand($stageTmpPath, $query)
    {
        $csvOptions = [];
        $csvOptions[] = sprintf('FIELD_DELIMITER = %s', $this->quote(CsvFile::DEFAULT_DELIMITER));
        $csvOptions[] = sprintf("FIELD_OPTIONALLY_ENCLOSED_BY = %s", $this->quote(CsvFile::DEFAULT_ENCLOSURE));
        $csvOptions[] = sprintf("ESCAPE_UNENCLOSED_FIELD = %s", $this->quote('\\'));
        $csvOptions[] = sprintf("COMPRESSION = %s", $this->quote('GZIP'));
        $csvOptions[] = sprintf("NULL_IF=()");

        return sprintf(
            "
            COPY INTO @~/%s/part
            FROM (%s)

            FILE_FORMAT = (TYPE=CSV %s)
            HEADER = false
            MAX_FILE_SIZE=50000000
            OVERWRITE = TRUE
            ;
            ",
            $stageTmpPath,
            rtrim(trim($query), ';'),
            implode(' ', $csvOptions)
        );
    }

    private function createTableManifest(array $table, array $columns): array
    {
        $manifestData = [
            'destination' => $table['outputTable'],
            'delimiter' => CsvFile::DEFAULT_DELIMITER,
            'enclosure' => CsvFile::DEFAULT_ENCLOSURE,
            'primary_key' => $table['primaryKey'],
            'incremental' => $table['incremental'],
            'columns' => $columns
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
                    $datatype = new SnowflakeDatatype(
                        $column['type'],
                        array_intersect_key($column, array_flip($datatypeKeys))
                    );
                    $columnMetadata[$column['name']] = $datatype->toMetadata();
                    $nonDatatypeKeys = array_diff_key($column, array_flip($datatypeKeys));
                    foreach ($nonDatatypeKeys as $key => $value) {
                        if ($key !== 'name') {
                            $columnMetadata[$column['name']][] = [
                                'key' => "KBC." . $key,
                                'value'=> $value
                            ];
                        }
                    }
                }
                unset($tableDetails['columns']);
                foreach ($tableDetails as $key => $value) {
                    $manifestData['metadata'][] = [
                        "key" => "KBC." . $key,
                        "value" => $value
                    ];
                }
                $manifestData['column_metadata'] = $columnMetadata;
            }
        }

        return $manifestData;
    }
    
    private function dataSizeFormatted(int $bytes)
    {
        $base = log($bytes) / log(1024);
        $suffixes = [' B', ' KB', ' MB', ' GB', ' TB'];
        return round(pow(1024, $base - floor($base)), 2) . $suffixes[(int) floor($base)];
    }

    public function getTables(array $tables = null)
    {
        $sql = "SHOW TABLES";
        $arr = $this->db->fetchAll($sql);

        $tableNameArray = [];
        $tableDefs = [];
        foreach ($arr as $table) {
            if (is_null($tables) || !(array_search($table['name'], array_column($tables, 'tableName')) === false)) {
                $tableNameArray[] = $table['name'];
                $tableDefs[$table['schema_name'] . '.' . $table['name']] = [
                    'name' => $table['name'],
                    'catalog' => (isset($table['database_name'])) ? $table['database_name'] : null,
                    'schema' => (isset($table['schema_name'])) ? $table['schema_name'] : null,
                    'type' => (isset($table['kind'])) ? $table['kind'] : null,
                    'rowCount' => (isset($table['rows'])) ? $table['rows'] : null,
                    'byteCount' => (isset($table['bytes'])) ? $table['bytes'] : null
                ];
            }
        }

        if (count($tableNameArray) === 0) {
            return [];
        }

        $sql = sprintf(
            "SELECT * FROM information_schema.columns 
             WHERE TABLE_NAME IN (%s) 
             ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION",
            implode(', ', array_map(function ($tableName) {
                return "'" . $tableName . "'";
            }, $tableNameArray))
        );

        $columns = $this->db->fetchAll($sql);
        foreach ($columns as $i => $column) {
            $curTable = $column['TABLE_SCHEMA'] . '.' . $column['TABLE_NAME'];
            $length = ($column['CHARACTER_MAXIMUM_LENGTH']) ? $column['CHARACTER_MAXIMUM_LENGTH'] : null;
            if (is_null($length) && !is_null($column['NUMERIC_PRECISION'])) {
                if (is_numeric($column['NUMERIC_SCALE'])) {
                    $length = $column['NUMERIC_PRECISION'] . "," . $column['NUMERIC_SCALE'];
                } else {
                    $length = $column['NUMERIC_PRECISION'];
                }
            }

            $curColumn = [
                "name" => $column['COLUMN_NAME'],
                "default" => $column['COLUMN_DEFAULT'],
                "length" => $length,
                "nullable" => (trim($column['IS_NULLABLE']) === "NO") ? false : true,
                "type" => $column['DATA_TYPE'],
                "ordinalPosition" => $column['ORDINAL_POSITION']
            ];

            if (!array_key_exists('columns', $tableDefs[$curTable])) {
                $tableDefs[$curTable]['columns'] = [];
            }
            $tableDefs[$curTable]['columns'][] = $curColumn;
        }
        return array_values($tableDefs);
    }

    public function describeTable(array $table)
    {
        // Deprecated
        return null;
    }

    public function simpleQuery(array $table, array $columns = array())
    {
        if (count($columns) > 0) {
            return sprintf(
                "SELECT %s FROM %s.%s",
                implode(', ', array_map(function ($column) {
                    return $this->db->quoteIdentifier($column);
                }, $columns)),
                $this->db->quoteIdentifier($table['schema']),
                $this->db->quoteIdentifier($table['tableName'])
            );
        } else {
            return sprintf(
                "SELECT * FROM %s.%s",
                $this->db->quoteIdentifier($table['schema']),
                $this->db->quoteIdentifier($table['tableName'])
            );
        }
    }

    /**
     * @param $output
     * @param $path
     * @return \SplFileInfo[]
     * @throws \Exception
     */
    private function parseFiles($output, $path)
    {
        $files = [];
        $lines = explode("\n", $output);

        $lines = array_map(
            function ($item) {
                $item = trim($item, '|');
                return array_map('trim', explode('|', $item));
            },
            array_filter(
                $lines,
                function ($item) {
                    $item = trim($item);
                    return preg_match('/^\|.+\|$/ui', $item) && preg_match('/([a-z0-9\_\-\.]+\.gz)/ui', $item);
                }
            )
        );

        foreach ($lines as $line) {
            if (!preg_match('/^downloaded$/ui', $line[2])) {
                throw new \Exception(sprintf(
                    "Cannot download file: %s Status: %s Size: %s Message: %s",
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
                throw new \Exception("Missing file: " . $line[0]);
            }
        }

        return $files;
    }

    private function quote($value)
    {
        return "'" . addslashes($value) . "'";
    }

    /**
     * @param $dbParams
     * @return \SplFileInfo
     */
    private function crateSnowSqlConfig($dbParams)
    {
        $cliConfig[] = '';
        $cliConfig[] = '[options]';
        $cliConfig[] = 'exit_on_error = true';
        $cliConfig[] = '';
        $cliConfig[] = '[connections.downloader]';
        $cliConfig[] = sprintf('accountname = "%s"', AccountUrlParser::parse($dbParams['host']));
        $cliConfig[] = sprintf('username = "%s"', $dbParams['user']);
        $cliConfig[] = sprintf('password = "%s"', $dbParams['password']);
        $cliConfig[] = sprintf('dbname = "%s"', $dbParams['database']);
        $cliConfig[] = sprintf('schemaname = "%s"', $dbParams['schema']);

        if (isset($dbParams['warehouse'])) {
            $cliConfig[] = sprintf('warehousename = "%s"', $dbParams['warehouse']);
        }


        $file = $this->temp->createFile('snowsql.config');
        file_put_contents($file, implode("\n", $cliConfig));

        return $file;
    }

    private function getUserDefaultWarehouse()
    {
        $sql = sprintf(
            "DESC USER %s;",
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

    private function execQuery($query)
    {
        try {
            $this->db->query($query);
        } catch (\Exception $e) {
            throw new UserException("Query execution error: " . $e->getMessage(), 0, $e);
        }
    }

    private function cleanupTableStage(string $tmpTableName): void
    {
        $sql = sprintf("REMOVE @~/%s;", $tmpTableName);
        $this->execQuery($sql);
    }
}
