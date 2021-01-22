<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\Component\Logger;
use Keboola\DbExtractor\Test\ExtractorTest;
use Keboola\DbExtractor\TraitTests\RemoveAllTablesTrait;
use Keboola\DbExtractor\TraitTests\Tables\AutoIncrementTableTrait;
use Keboola\DbExtractor\TraitTests\Tables\EscapingTableTrait;
use Keboola\DbExtractor\TraitTests\Tables\SalesTableTrait;
use Keboola\DbExtractor\TraitTests\Tables\TypesTableTrait;
use Keboola\SnowflakeDbAdapter\Connection;
use Keboola\SnowflakeDbAdapter\QueryBuilder;
use Keboola\DbExtractor\SnowflakeApplication;
use Symfony\Component\Filesystem\Filesystem;

abstract class AbstractSnowflakeTest extends ExtractorTest
{
    use RemoveAllTablesTrait;
    use AutoIncrementTableTrait;
    use EscapingTableTrait;
    use SalesTableTrait;
    use TypesTableTrait;

    public const DRIVER = 'snowflake';

    protected Connection $connection;

    protected string $dataDir = __DIR__ . '/data';

    public function setUp(): void
    {
        parent::setUp();

        if (!defined('APP_NAME')) {
            define('APP_NAME', 'ex-db-snowflake');
        }

        $config = $this->getConfig();

        $config['parameters']['db']['password'] = $config['parameters']['db']['#password'];
        unset($config['parameters']['db']['#password']);
        $this->connection = new Connection($config['parameters']['db']);

        $this->connection->query(
            sprintf('USE SCHEMA %s', QueryBuilder::quoteIdentifier($config['parameters']['db']['schema']))
        );
        $this->connection->query(
            'alter session set client_timestamp_type_mapping=\'timestamp_ntz\''
        );

        $this->setupTables();

        $fileSystem = new Filesystem();
        $fileSystem->remove($this->dataDir . '/out');
        $fileSystem->remove($this->dataDir . '/runAction/config.json');
        $fileSystem->remove($this->dataDir . '/runAction/out');
        $fileSystem->remove($this->dataDir . '/connectionAction/config.json');
        $fileSystem->remove($this->dataDir . '/connectionAction/out');
        $fileSystem->remove($this->dataDir . '/getTablesAction/config.json');
        $fileSystem->remove($this->dataDir . '/getTablesAction/out');
    }

    public function tearDown(): void
    {
        $this->connection->disconnect();
    }

    public function getConfig(string $driver = 'snowflake'): array
    {
        $config = parent::getConfig($driver);

        $config['parameters']['db']['schema'] = $this->getEnv($driver, 'DB_SCHEMA');
        $config['parameters']['db']['warehouse'] = $this->getEnv($driver, 'DB_WAREHOUSE');

        $config['parameters']['extractor_class'] = 'Snowflake';
        $config['parameters']['tables'][2]['table']['schema'] = $this->getEnv($driver, 'DB_SCHEMA');

        return $config;
    }

    public function getConfigRow(string $driver = 'snowflake'): array
    {
        $config = parent::getConfigRow($driver);

        $config['parameters']['db']['schema'] = $this->getEnv($driver, 'DB_SCHEMA');
        $config['parameters']['db']['warehouse'] = $this->getEnv($driver, 'DB_WAREHOUSE');

        $config['parameters']['extractor_class'] = 'Snowflake';
        $config['parameters']['table']['schema'] = $this->getEnv($driver, 'DB_SCHEMA');

        return $config;
    }

    public function createApplication(array $config, array $state = []): SnowflakeApplication
    {
        $logger = new Logger();
        return new SnowflakeApplication($config, $logger, $state, $this->dataDir);
    }

    private function setupTables(): void
    {
        $this->removeAllTables();
        $this->createTypesTable();
        $this->generateTypesRows();
        $this->createSalesTable();
        $this->generateSalesRows();
        $this->createEscapingTable();
        $this->generateEscapingRows();

        $this->connection->query('CREATE VIEW "escaping_view" AS SELECT * FROM "escaping"');

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

    protected function getIncrementalConfig(): array
    {
        $config = $this->getConfigRow(self::DRIVER);
        unset($config['parameters']['query']);
        unset($config['parameters']['columns']);
        $config['parameters']['table']['tableName'] = 'auto_increment_timestamp';
        $config['parameters']['incremental'] = true;
        $config['parameters']['name'] = 'auto-increment-timestamp';
        $config['parameters']['outputTable'] = 'in.c-main.auto-increment-timestamp';
        $config['parameters']['primaryKey'] = ['id'];
        $config['parameters']['incrementalFetchingColumn'] = 'id';

        return $config;
    }

    protected function createAutoIncrementAndTimestampTable(array $config): void
    {
        $this->dropAutoIncrementTable($config);

        $createQuery = sprintf(
            'CREATE TABLE %s.%s (
            "id" INT NOT NULL AUTOINCREMENT,
            "name" VARCHAR(30) NOT NULL DEFAULT \'pam\',
            "number" DECIMAL(10,8) NOT NULL DEFAULT 0.0,
            "timestamp" TIMESTAMP DEFAULT to_timestamp_ntz(current_timestamp()),
            "date" DATE DEFAULT CURRENT_DATE,
            "datetime" DATETIME NOT NULL DEFAULT to_timestamp_ntz(current_timestamp()),
            PRIMARY KEY ("id")
            )',
            QueryBuilder::quoteIdentifier($config['parameters']['table']['schema']),
            QueryBuilder::quoteIdentifier($config['parameters']['table']['tableName'])
        );
        $this->connection->query($createQuery);

        $insertQuery = sprintf(
            'INSERT INTO %s.%s ("name", "date") VALUES (\'george\', \'2019-11-20\'), (\'henry\', \'2019-11-21\')',
            QueryBuilder::quoteIdentifier($config['parameters']['table']['schema']),
            QueryBuilder::quoteIdentifier($config['parameters']['table']['tableName'])
        );
        $this->connection->query($insertQuery);
    }

    protected function dropAutoIncrementTable(array $config): void
    {
        $dropQuery = sprintf(
            'DROP TABLE IF EXISTS %s.%s',
            QueryBuilder::quoteIdentifier($config['parameters']['table']['schema']),
            QueryBuilder::quoteIdentifier($config['parameters']['table']['tableName'])
        );
        $this->connection->query($dropQuery);
    }
}
