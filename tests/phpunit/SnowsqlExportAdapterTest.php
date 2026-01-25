<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Adapter\ODBC\OdbcConnection;
use Keboola\DbExtractor\Configuration\ValueObject\SnowflakeDatabaseConfig;
use Keboola\DbExtractor\Extractor\SnowflakeMetadataProvider;
use Keboola\DbExtractor\Extractor\SnowflakeQueryFactory;
use Keboola\DbExtractor\Extractor\SnowsqlExportAdapter;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;
use ReflectionClass;

class SnowsqlExportAdapterTest extends TestCase
{
    /**
     * @dataProvider cleanupTableStageDataProvider
     */
    public function testCleanupTableStageUsesTrailingSlash(string $tableName, string $expectedSql): void
    {
        $logger = new TestLogger();

        $connection = $this->createMock(OdbcConnection::class);
        $connection
            ->expects($this->once())
            ->method('query')
            ->with($expectedSql);

        $queryFactory = $this->createMock(SnowflakeQueryFactory::class);
        $metadataProvider = $this->createMock(SnowflakeMetadataProvider::class);

        $databaseConfig = $this->createMock(SnowflakeDatabaseConfig::class);
        $databaseConfig->method('getHost')->willReturn('test.snowflakecomputing.com');
        $databaseConfig->method('getUsername')->willReturn('testuser');
        $databaseConfig->method('getPassword')->willReturn('testpass');
        $databaseConfig->method('getDatabase')->willReturn('testdb');
        $databaseConfig->method('hasWarehouse')->willReturn(false);
        $databaseConfig->method('hasSchema')->willReturn(false);
        $databaseConfig->method('hasPrivateKey')->willReturn(false);

        $adapter = new SnowsqlExportAdapter(
            $logger,
            $connection,
            $queryFactory,
            $metadataProvider,
            $databaseConfig,
        );

        $reflection = new ReflectionClass($adapter);
        $method = $reflection->getMethod('cleanupTableStage');
        $method->setAccessible(true);
        $method->invoke($adapter, $tableName);
    }

    public function cleanupTableStageDataProvider(): array
    {
        return [
            'simple table name' => [
                'my_table',
                'REMOVE @~/my_table/;',
            ],
            'table name with prefix that could match other tables' => [
                'r_executive_brand_revenue',
                'REMOVE @~/r_executive_brand_revenue/;',
            ],
            'table name that is prefix of another' => [
                'orders',
                'REMOVE @~/orders/;',
            ],
            'table name with dots' => [
                'in.c-main.escaping',
                'REMOVE @~/in.c-main.escaping/;',
            ],
            'table name with special characters' => [
                'my-table_123',
                'REMOVE @~/my-table_123/;',
            ],
        ];
    }
}
