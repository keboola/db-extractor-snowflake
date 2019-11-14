<?php

namespace Keboola\DbExtractor\Tests;

class SnowflakeIncrementalTest extends AbstractSnowflakeTest
{
    public const DRIVER = 'snowflake';

    public const ROOT_PATH = __DIR__ . '/..';

    public function testIncrementalFetching(): void
    {
        $config = $this->getIncrementalConfig();
        $this->createAutoIncrementAndTimestampTable($config);

        $app = $this->createApplication($config);
        $result = $app->run();

        var_dump($result);
    }

    private function getIncrementalConfig(): array
    {
        $config = $this->getConfig(self::DRIVER);
        $tables = $config['parameters']['tables'];
        $tables = array_filter($tables, function ($k) {
            if ($k > 0) {
                return false;
            }
            return true;
        }, ARRAY_FILTER_USE_KEY);

        $tables = array_map(function ($item) {
            unset($item['query']);
            unset($item['columns']);
            $item['table'] = [
                'tableName' => 'auto_increment_timestamp',
                'schema' => 'testdb',
            ];
            $item['incremental'] = true;
            $item['name'] = 'auto-increment-timestamp';
            $item['outputTable'] = 'in.c-main.auto-increment-timestamp';
            $item['primaryKey'] = ['id'];
            $item['incrementalFetchingColumn'] = 'id';
            return $item;
        }, $tables);

        $config['parameters']['tables'] = $tables;
        return $config;
    }

    private function createAutoIncrementAndTimestampTable(array $config): void
    {
        $this->connection->query(sprintf(
            'DROP TABLE IF EXISTS %s.%s',
            $this->connection->quoteIdentifier($config['parameters']['db']['schema']),
            $config['parameters']['tables'][0]['table']['tableName']
        ));

        $this->connection->query(sprintf(
    'CREATE TABLE %s.%s (
            `id` INT NOT NULL AUTOINCREMENT,
            `name` VARCHAR(30) NOT NULL DEFAULT \'pam\',
            `number` FLOAT NOT NULL DEFAULT 0.0,
            `timestamp` TIMESTAMP DEFAULT to_timestamp_ntz(current_timestamp()),
            PRIMARY KEY (`id`)
            )',
            $this->connection->quoteIdentifier($config['parameters']['db']['schema']),
            $config['parameters']['tables'][0]['table']['tableName']
        ));

        $this->connection->query(sprintf(
            'INSERT INTO %s.%s (`name`) VALUES (\'george\'), (\'henry\')',
            $this->connection->quoteIdentifier($config['parameters']['db']['schema']),
            $config['parameters']['tables'][0]['table']['tableName']
        ));
    }
}