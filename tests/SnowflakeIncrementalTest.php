<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

class SnowflakeIncrementalTest extends AbstractSnowflakeTest
{

    public const ROOT_PATH = __DIR__ . '/..';

    public function testIncrementalFetchingByTimestamp(): void
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

    public function testIncrementalFetchingByDatetime(): void
    {
        $config = $this->getIncrementalConfig();
        $config['parameters']['incrementalFetchingColumn'] = 'datetime';
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

    public function testIncrementalFetchingLimit(): void
    {
        $config = $this->getIncrementalConfig();
        $config['parameters']['incrementalFetchingLimit'] = 1;
        $this->createAutoIncrementAndTimestampTable($config);

        $app = $this->createApplication($config);
        $result = $app->run();

        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 1,
            ],
            $result['imported']
        );

        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals(1, $result['state']['lastFetchedRow']);

        // since it's >= we'll set limit to 2 to fetch the second row also
        $config['parameters']['incrementalFetchingLimit'] = 2;
        $app = $this->createApplication($config, $result['state']);
        $result = $app->run();

        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 2,
            ],
            $result['imported']
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
        $this->assertEquals(3, $newResult['state']['lastFetchedRow']);
        $this->assertGreaterThan(
            $result['state']['lastFetchedRow'],
            $newResult['state']['lastFetchedRow'],
        );
        $this->assertEquals(2, $newResult['imported']['rows']);

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
}
