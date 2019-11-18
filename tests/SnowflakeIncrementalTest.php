<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

class SnowflakeIncrementalTest extends AbstractSnowflakeTest
{
    public const DRIVER = 'snowflake';

    public const ROOT_PATH = __DIR__ . '/..';

    public function testIncrementalFetchingByDatetime(): void
    {
        $config = $this->getIncrementalConfig();
        $config['parameters']['incrementalFetchingColumn'] = 'timestamp';
        $this->createAutoIncrementAndTimestampTable($config);

        $app = $this->createApplication($config);
        $result = $app->run();

        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 2,
            ],
            $result['imported']
        );

        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertNotEmpty($result['state']['lastFetchedRow']);

        $app = $this->createApplication($config, $result['state']);
        $emtpyResult = $app->run();
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 2,
            ],
            $emtpyResult['imported']
        );

        $this->connection->query(sprintf(
            "INSERT INTO %s.%s (\"name\") VALUES ('wiliam'), ('charles')",
            $this->connection->quoteIdentifier($config['parameters']['table']['schema']),
            $this->connection->quoteIdentifier($config['parameters']['table']['tableName'])
        ));

        $app = $this->createApplication($config, $result['state']);
        $newResult = $app->run();

        $this->assertArrayHasKey('state', $newResult);
        $this->assertArrayHasKey('lastFetchedRow', $newResult['state']);
        $this->assertNotEmpty($newResult['state']['lastFetchedRow']);
        $this->assertGreaterThan(
            $result['state']['lastFetchedRow'],
            $newResult['state']['lastFetchedRow'],
            );
        $this->assertEquals(4, $newResult['imported']['rows']);

        $this->dropAutoIncrementTable($config);
    }

    public function testIncrementalFetchingByAutoIncrement(): void
    {
        $config = $this->getIncrementalConfig();
        $this->createAutoIncrementAndTimestampTable($config);

        $app = $this->createApplication($config);
        $result = $app->run();

        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 2,
            ],
            $result['imported']
        );

        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals(2, $result['state']['lastFetchedRow']);

        $app = $this->createApplication($config, $result['state']);
        $emtpyResult = $app->run();
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 1,
            ],
            $emtpyResult['imported']
        );

        $this->connection->query(sprintf(
            "INSERT INTO %s.%s (\"name\") VALUES ('wiliam'), ('charles')",
            $this->connection->quoteIdentifier($config['parameters']['table']['schema']),
            $this->connection->quoteIdentifier($config['parameters']['table']['tableName'])
        ));

        $app = $this->createApplication($config, $result['state']);
        $newResult = $app->run();

        $this->assertArrayHasKey('state', $newResult);
        $this->assertArrayHasKey('lastFetchedRow', $newResult['state']);
        $this->assertEquals(4, $newResult['state']['lastFetchedRow']);
        $this->assertGreaterThan(
            $result['state']['lastFetchedRow'],
            $newResult['state']['lastFetchedRow'],
            );
        $this->assertEquals(3, $newResult['imported']['rows']);

        $this->dropAutoIncrementTable($config);
    }

    public function testIncrementalFetchingByDecimal(): void
    {
        $config = $this->getIncrementalConfig();
        $config['parameters']['incrementalFetchingColumn'] = 'number';
        $this->createAutoIncrementAndTimestampTable($config);

        $app = $this->createApplication($config);
        $result = $app->run();

        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 2,
            ],
            $result['imported']
        );

        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertNotEmpty($result['state']['lastFetchedRow']);

        $app = $this->createApplication($config, $result['state']);
        $emtpyResult = $app->run();
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 2,
            ],
            $emtpyResult['imported']
        );

        $this->connection->query(sprintf(
            "INSERT INTO %s.%s (\"name\", \"number\") VALUES ('wiliam', '20.548796'), ('charles', '35.5478524')",
            $this->connection->quoteIdentifier($config['parameters']['table']['schema']),
            $this->connection->quoteIdentifier($config['parameters']['table']['tableName'])
        ));

        $app = $this->createApplication($config, $result['state']);
        $newResult = $app->run();

        $this->assertArrayHasKey('state', $newResult);
        $this->assertArrayHasKey('lastFetchedRow', $newResult['state']);
        $this->assertNotEmpty($newResult['state']['lastFetchedRow']);
        $this->assertGreaterThan(
            $result['state']['lastFetchedRow'],
            $newResult['state']['lastFetchedRow'],
            );
        $this->assertEquals(35.5478524, $newResult['state']['lastFetchedRow']);
        $this->assertEquals(4, $newResult['imported']['rows']);

        $this->dropAutoIncrementTable($config);
    }

    private function getIncrementalConfig(): array
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

    private function createAutoIncrementAndTimestampTable(array $config): void
    {
        $this->dropAutoIncrementTable($config);

        $createQuery = sprintf(
            'CREATE TABLE %s.%s (
            "id" INT NOT NULL AUTOINCREMENT,
            "name" VARCHAR(30) NOT NULL DEFAULT \'pam\',
            "number" DECIMAL(10,8) NOT NULL DEFAULT 0.0,
            "timestamp" TIMESTAMP DEFAULT to_timestamp_ntz(current_timestamp()),
            PRIMARY KEY ("id")
            )',
            $this->connection->quoteIdentifier($config['parameters']['table']['schema']),
            $this->connection->quoteIdentifier($config['parameters']['table']['tableName'])
        );
        $this->connection->query($createQuery);

        $insertQuery = sprintf(
            'INSERT INTO %s.%s ("name") VALUES (\'george\'), (\'henry\')',
            $this->connection->quoteIdentifier($config['parameters']['table']['schema']),
            $this->connection->quoteIdentifier($config['parameters']['table']['tableName'])
        );
        $this->connection->query($insertQuery);
    }

    private function dropAutoIncrementTable(array $config): void
    {
        $dropQuery = sprintf(
            'DROP TABLE IF EXISTS %s.%s',
            $this->connection->quoteIdentifier($config['parameters']['table']['schema']),
            $this->connection->quoteIdentifier($config['parameters']['table']['tableName'])
        );
        $this->connection->query($dropQuery);
    }
}
