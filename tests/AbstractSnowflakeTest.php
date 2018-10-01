<?php

declare(strict_types=1);

namespace Keboola\Test;

use Keboola\Csv\CsvFile;
use Keboola\Db\Import\Snowflake\Connection;
use Keboola\DbExtractor\Logger;
use Keboola\DbExtractor\Test\ExtractorTest;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\GetFileOptions;
use Keboola\DbExtractor\SnowflakeApplication;
use Symfony\Component\Filesystem\Filesystem;

abstract class AbstractSnowflakeTest extends ExtractorTest
{
    /**
     * @var Connection
     */
    protected $connection;

    /**
     * @var Client -- sapi client
     */
    protected $storageApiClient;

    /** @var string  */
    protected $dataDir = __DIR__ . '/data';

    public function setUp(): void
    {
        parent::setUp();

        if (!defined('APP_NAME')) {
            define('APP_NAME', 'ex-db-snowflake');
        }

        $config = $this->getConfig();

        $this->connection = new Connection($config['parameters']['db']);

        $this->connection->query(
            sprintf("USE SCHEMA %s", $this->connection->quoteIdentifier($config['parameters']['db']['schema']))
        );

        $this->storageApiClient = new Client(
            [
            'token' => getenv('STORAGE_API_TOKEN'),
            ]
        );

        $this->setupTables();

        $fileSystem = new Filesystem();
        $fileSystem->remove(__DIR__ . '/data/out');
        $fileSystem->remove(__DIR__ . '/data/runAction/config.yml');
        $fileSystem->remove(__DIR__ . '/data/runAction/out');
        $fileSystem->remove(__DIR__ . '/data/connectionAction/config.yml');
        $fileSystem->remove(__DIR__ . '/tests/data/connectionAction/out');
    }

    public function getConfig(string $driver = 'snowflake', string $format = 'json'): array
    {
        $config = parent::getConfig($driver);

        $config['parameters']['db']['schema'] = $this->getEnv($driver, 'DB_SCHEMA');
        $config['parameters']['db']['warehouse'] = $this->getEnv($driver, 'DB_WAREHOUSE');

        if (!empty($config['parameters']['db']['#password'])) {
            $config['parameters']['db']['password'] = $config['parameters']['db']['#password'];
        }
        $config['parameters']['extractor_class'] = 'Snowflake';
        $config['parameters']['tables'][2]['table']['schema'] = $this->getEnv($driver, 'DB_SCHEMA');

        return $config;
    }

    /**
     * @param  array $config
     * @return SnowflakeApplication
     */
    public function createApplication(array $config): SnowflakeApplication
    {
        $logger = new Logger('ex-db-snowflake-tests');
        $app = new SnowflakeApplication($config, $logger, [], $this->dataDir);

        return $app;
    }

    protected function generateTableName(CsvFile $file): string
    {
        $tableName = sprintf(
            '%s',
            $file->getBasename('.' . $file->getExtension())
        );

        return $tableName;
    }

    private function setupTables(): void
    {
        $salescsv = new CsvFile($this->dataDir . '/snowflake/sales.csv');
        $this->createTextTable($salescsv);

        $escaping = new CsvFile($this->dataDir . '/snowflake/escaping.csv');
        $this->createTextTable($escaping);

        $types = new CsvFile($this->dataDir . '/snowflake/types.csv');

        $this->connection->query(
            sprintf(
                'DROP TABLE IF EXISTS %s',
                $this->connection->quoteIdentifier('types')
            )
        );

        $this->connection->query(
            sprintf(
                'CREATE TABLE %s (
                  "character" VARCHAR(100) NOT NULL, 
                  "integer" NUMBER(6,0), 
                  "decimal" NUMBER(10,2), 
                  "date" DATE
                );',
                $this->connection->quoteIdentifier('types')
            )
        );
        $storageFileInfo = $this->storageApiClient->getFile(
            $this->storageApiClient->uploadFile(
                (string) $types,
                new FileUploadOptions()
            ),
            (new GetFileOptions())->setFederationToken(true)
        );
        $createTableCommand = $this->generateCreateCommand('types', $types, $storageFileInfo);
        $this->connection->query($createTableCommand);

        // drop view if it exists
        $this->connection->query('DROP VIEW IF EXISTS "escaping_view"');
        // create a view
        $this->connection->query('CREATE VIEW "escaping_view" AS SELECT * FROM "escaping"');

        // create a view with a json object column
        $this->connection->query('DROP TABLE IF EXISTS "semi-structured"');
        $this->connection->query(
            'CREATE TABLE "semi-structured" ( 
                                            "var" VARIANT, 
                                            "obj" OBJECT,
                                            "arr" ARRAY
                                       )'
        );
        $this->connection->query(
            'INSERT INTO "semi-structured" 
                  SELECT 
                      OBJECT_CONSTRUCT(\'a\', 1, \'b\', \'BBBB\', \'c\', null) AS "var",
                      OBJECT_CONSTRUCT(\'a\', 1, \'b\', \'BBBB\', \'c\', null) AS "org",
                      ARRAY_CONSTRUCT(10, 20, 30) AS "arr";'
        );
    }

    private function quote(string $value): string
    {
        return "'" . addslashes($value) . "'";
    }

    private function generateCreateCommand(string $tableName, CsvFile $csv, \SplFileInfo $fileInfo): string
    {
        $csvOptions = [];
        $csvOptions[] = sprintf('FIELD_DELIMITER = %s', $this->connection->quoteIdentifier($csv->getDelimiter()));
        $csvOptions[] = sprintf("FIELD_OPTIONALLY_ENCLOSED_BY = %s", $this->quote($csv->getEnclosure()));
        $csvOptions[] = sprintf("ESCAPE_UNENCLOSED_FIELD = %s", $this->connection->quoteIdentifier("\\"));

        if (!$fileInfo['isSliced']) {
            $csvOptions[] = "SKIP_HEADER = 1";
        }

        return sprintf(
            "
             COPY INTO %s
             FROM 's3://%s/%s'
             FILE_FORMAT = (TYPE=CSV %s)
             CREDENTIALS = (AWS_KEY_ID = %s AWS_SECRET_KEY = %s  AWS_TOKEN = %s)
            ",
            $this->connection->quoteIdentifier($tableName),
            $fileInfo['s3Path']['bucket'],
            $fileInfo['s3Path']['key'],
            implode(' ', $csvOptions),
            $this->quote($fileInfo['credentials']['AccessKeyId']),
            $this->quote($fileInfo['credentials']['SecretAccessKey']),
            $this->quote($fileInfo['credentials']['SessionToken'])
        );
    }

    /**
     * Create table from csv file with text columns
     *
     * @param CsvFile $file
     * @param string  $tableName  - optional name override (default uses filename)
     * @param string  $schemaName - optional schema in which to create the table
     */
    protected function createTextTable(CsvFile $file, ?string $tableName = null, ?string $schemaName = null): void
    {
        if (!$tableName) {
            $tableName = $this->generateTableName($file);
        }

        if ($schemaName) {
            $this->connection->query(
                sprintf("USE SCHEMA %s", $this->connection->quoteIdentifier($schemaName))
            );
        }

        $this->connection->query(
            sprintf(
                'DROP TABLE IF EXISTS %s',
                $this->connection->quoteIdentifier($tableName)
            )
        );

        $this->connection->query(
            sprintf(
                'CREATE TABLE %s (%s);',
                $this->connection->quoteIdentifier($tableName),
                implode(
                    ', ',
                    array_map(
                        function ($column) {
                            $q = '"';
                            return ($q . str_replace("$q", "$q$q", $column) . $q) . ' VARCHAR(200) NOT NULL';
                        },
                        $file->getHeader()
                    )
                ),
                $tableName
            )
        );

        $storageFileInfo = $this->storageApiClient->getFile(
            $this->storageApiClient->uploadFile(
                (string) $file,
                new FileUploadOptions()
            ),
            (new GetFileOptions())->setFederationToken(true)
        );

        $sql = $this->generateCreateCommand($tableName, $file, $storageFileInfo);
        $this->connection->query($sql);

        $sql = sprintf(
            'SELECT COUNT(*) AS ROWCOUNT FROM %s',
            $this->connection->quoteIdentifier($tableName)
        );
        $result = $this->connection->fetchAll($sql);
        $this->assertEquals($this->countTable($file), (int) $result[0]['ROWCOUNT']);
    }

    /**
     * Count records in CSV (with headers)
     *
     * @param  CsvFile $file
     * @return int
     */
    protected function countTable(CsvFile $file): int
    {
        $linesCount = 0;
        foreach ($file as $i => $line) {
            // skip header
            if (!$i) {
                continue;
            }

            $linesCount++;
        }

        return $linesCount;
    }

    public function configTypesProvider(): array
    {
        return [
            ['yaml'],
            ['json'],
        ];
    }
}
