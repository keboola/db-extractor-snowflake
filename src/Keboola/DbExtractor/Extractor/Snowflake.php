<?php
namespace Keboola\DbExtractor\Extractor;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Logger;
use Keboola\DbExtractor\Snowflake\Connection;
use Keboola\Temp\Temp;
use Symfony\Component\Process\Process;
use Symfony\Component\Yaml\Yaml;

class Snowflake extends Extractor
{
    const STATEMENT_TIMEOUT_IN_SECONDS = 900;

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
        $connection->query(sprintf("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = %d", self::STATEMENT_TIMEOUT_IN_SECONDS));

        $this->database = $dbParams['database'];
        $this->schema = $dbParams['schema'];

        if (!empty($dbParams['warehouse'])) {
            $this->warehouse = $dbParams['warehouse'];
        }

        return $connection;
    }

    private function quote($value)
    {
        return "'" . addslashes($value) . "'";
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
                throw new \Exception("Cannot download file: " . $line[0]);
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

    private function exportAndDownload(array $table)
    {
        $sql = sprintf("REMOVE @%s/%s;", $this->generateStageName(), str_replace('.', '_', $table['outputTable']));
        $this->execQuery($sql);

        $csvOptions = [];
        $csvOptions[] = sprintf('FIELD_DELIMITER = %s', $this->quote(CsvFile::DEFAULT_DELIMITER));
        $csvOptions[] = sprintf("FIELD_OPTIONALLY_ENCLOSED_BY = %s", $this->quote(CsvFile::DEFAULT_ENCLOSURE));
        $csvOptions[] = sprintf("ESCAPE_UNENCLOSED_FIELD = %s", $this->quote('\\'));
        $csvOptions[] = sprintf("COMPRESSION = %s", $this->quote('GZIP'));
        $csvOptions[] = sprintf("NULL_IF=()");

        $sql = sprintf(
            "
            COPY INTO @%s/%s
            FROM (%s)

            FILE_FORMAT = (TYPE=CSV %s)
            HEADER = true
            MAX_FILE_SIZE=50000000
            OVERWRITE = TRUE
            ;
            ",
            $this->generateStageName(),
            str_replace('.', '_', $table['outputTable']),
            rtrim(trim($table['query']), ';'),
            implode(' ', $csvOptions)
        );

        $this->execQuery($sql);

        $this->logger->info("Downloading data from Snowflake");

        @mkdir($this->dataDir . '/out/tables', 0770, true);

        $sql = [];
        $sql[] = sprintf('USE DATABASE %s;', $this->db->quoteIdentifier($this->database));
        $sql[] = sprintf('USE SCHEMA %s;', $this->db->quoteIdentifier($this->schema));

        if ($this->warehouse) {
            $sql[] = sprintf('USE WAREHOUSE %s;', $this->db->quoteIdentifier($this->warehouse));
        }

        $sql[] = sprintf(
            'GET @%s/%s file://%s;',
            $this->generateStageName(),
            str_replace('.', '_', $table['outputTable']),
            $this->dataDir . '/out/tables/'
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


        $process = new Process($command, null, null, null, self::STATEMENT_TIMEOUT_IN_SECONDS);
        $process->run();

        if (!$process->isSuccessful()) {
            throw new \Exception("File download error occurred");
        }

        $csvFiles = $this->parseFiles($process->getOutput(), $this->dataDir . '/out/tables');
        $bytes = 0;
        foreach ($csvFiles as $csvFile) {
            $bytes += $csvFile->getSize();

            $manifestData = [
                'destination' => $table['outputTable'],
                'delimiter' => CsvFile::DEFAULT_DELIMITER,
                'enclosure' => CsvFile::DEFAULT_ENCLOSURE,
                'primary_key' => $table['primaryKey'],
                'incremental' => $table['incremental'],

            ];

            file_put_contents($csvFile . '.manifest', Yaml::dump($manifestData));
        }

        $base = log($bytes) / log(1024);
        $suffixes = array(' B', ' KB', ' MB', ' GB', ' TB');
        $bytes =  round(pow(1024, $base - floor($base)), 2) . $suffixes[floor($base)];
        $this->logger->info(sprintf("%d files (%s) downloaded", count($csvFiles), $bytes));
    }

    private function generateStageName()
    {
        return '~';
    }

    /**
     * @param $dbParams
     * @return \SplFileInfo
     */
    private function crateSnowSqlConfig($dbParams)
    {
        $hostParts = explode('.', $dbParams['host']);

        $cliConfig[] = '';
        $cliConfig[] = '[options]';
        $cliConfig[] = 'exit_on_error = true';
        $cliConfig[] = '';
        $cliConfig[] = '[connections.downloader]';
        $cliConfig[] = sprintf('accountname = %s', reset($hostParts));
        $cliConfig[] = sprintf('username = %s', $dbParams['user']);
        $cliConfig[] = sprintf('password = %s', $dbParams['password']);
        $cliConfig[] = sprintf('dbname = %s', $dbParams['database']);
        $cliConfig[] = sprintf('schemaname = %s', $dbParams['schema']);
        $cliConfig[] = sprintf('warehousename = %s', $dbParams['user']);

        $file = $this->temp->createFile('snowsql.config');
        file_put_contents($file, implode("\n", $cliConfig));

        return $file;
    }

    public function export(array $table)
    {
        $outputTable = $table['outputTable'];

        $this->logger->info("Exporting to " . $outputTable);

        $this->exportAndDownload($table);

        return $outputTable;
    }

    protected function executeQuery($query, CsvFile $csv)
    {
    }

    public function testConnection()
    {
        $this->db->query('SELECT current_date;');
    }

    private function execQuery($query)
    {
        $logQuery = $this->hideCredentialsInQuery($query);
        $logQuery = trim(preg_replace('/\s+/', ' ', $logQuery));

        $this->logger->info(sprintf("Executing query '%s'", $logQuery));
        try {
            $this->db->query($query);
        } catch (\Exception $e) {
            throw new UserException("Query execution error: " . $e->getMessage(), 0, $e);
        }
    }

    private function hideCredentialsInQuery($query)
    {
        return preg_replace("/(AWS_[A-Z_]*\\s=\\s.)[0-9A-Za-z\\/\\+=]*./", '${1}...\'', $query);
    }
}
