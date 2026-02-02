<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Adapter\ODBC\OdbcConnection;
use Keboola\DbExtractor\Configuration\ValueObject\SnowflakeDatabaseConfig;
use Keboola\DbExtractor\Extractor\SnowflakeConnectionFactory;
use Keboola\DbExtractor\Extractor\SnowflakeMetadataProvider;
use Keboola\DbExtractor\Extractor\SnowflakeQueryFactory;
use Keboola\DbExtractor\Extractor\SnowsqlExportAdapter;
use Keboola\DbExtractor\FunctionalTests\DatabaseManager;
use Keboola\DbExtractor\FunctionalTests\TestConnection;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use Psr\Log\Test\TestLogger;
use ReflectionClass;
use Symfony\Component\Process\Process;
use Throwable;

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

    /**
     * @dataProvider generateDownloadSqlDataProvider
     */
    public function testGenerateDownloadSqlUsesTrailingSlash(string $tableName): void
    {
        $logger = new TestLogger();

        $connection = $this->createMock(OdbcConnection::class);
        $connection->method('quoteIdentifier')->willReturnCallback(fn($val) => '"' . $val . '"');

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

        $exportConfig = $this->createMock(ExportConfig::class);
        $exportConfig->method('getOutputTable')->willReturn($tableName);

        $reflection = new ReflectionClass($adapter);
        $method = $reflection->getMethod('generateDownloadSql');
        $method->setAccessible(true);
        $method->invoke($adapter, $exportConfig, '/tmp/output');

        // Check that the debug log contains GET command with trailing slash
        $debugLogs = array_filter($logger->records, fn($record) => $record['level'] === 'debug');
        $this->assertNotEmpty($debugLogs, 'Expected debug log to be recorded');

        $logMessage = end($debugLogs)['message'];
        $expectedGetCommand = sprintf('GET @~/%s/ file:///tmp/output;', $tableName);
        $this->assertStringContainsString(
            $expectedGetCommand,
            $logMessage,
            sprintf('Expected GET command with trailing slash for table "%s"', $tableName),
        );
    }

    public function generateDownloadSqlDataProvider(): array
    {
        return [
            'simple table name' => ['my_table'],
            'table name with prefix that could match other tables' => ['r_executive_brand_revenue'],
            'table name that is prefix of another' => ['orders'],
            'table name with dots' => ['in.c-main.escaping'],
            'table name with special characters' => ['my-table_123'],
        ];
    }

    public function testGetCommandWithTrailingSlashPreventsDownloadingPrefixMatchedFiles(): void
    {
        $connection = TestConnection::createConnection();
        $temp = new Temp();

        $schema = (string) getenv('SNOWFLAKE_DB_SCHEMA');
        $manager = new DatabaseManager($connection);

        // Create and populate first table: "simple"
        $manager->createTable(
            'simple',
            ['id' => 'NUMBER', 'name' => 'VARCHAR'],
            $schema,
        );
        $manager->insertRows(
            'simple',
            ['id', 'name'],
            [
                [1, 'foo'],
                [2, 'bar'],
                [3, 'baz'],
            ],
        );

        // Create and populate second table: "simple-date" (prefix matches "simple")
        $manager->createTable(
            'simple-date',
            ['id' => 'NUMBER', 'name' => 'VARCHAR', 'created_at' => 'DATE'],
            $schema,
        );
        $manager->insertRows(
            'simple-date',
            ['id', 'name', 'created_at'],
            [
                [1, 'alpha', '2024-01-01'],
                [2, 'beta', '2024-01-02'],
            ],
        );

        // Manually stage BOTH tables simultaneously (simulates parallel runs)
        // This is the key difference from normal functional tests
        $stageName1 = 'in.c-main.simple';
        $stageName2 = 'in.c-main.simple-date';

        // Clean up any existing stage files first
        try {
            $connection->query("REMOVE @~/$stageName1/;");
        } catch (Throwable $e) {
            // Ignore errors if stage doesn't exist
        }
        try {
            $connection->query("REMOVE @~/$stageName2/;");
        } catch (Throwable $e) {
            // Ignore errors if stage doesn't exist
        }

        // Upload table 1 to stage
        $connection->query("
            COPY INTO @~/$stageName1/part
            FROM (SELECT * FROM \"simple\")
            FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER=',' COMPRESSION='GZIP')
            OVERWRITE = TRUE;
        ");

        // Upload table 2 to stage
        $connection->query("
            COPY INTO @~/$stageName2/part
            FROM (SELECT * FROM \"simple-date\")
            FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER=',' COMPRESSION='GZIP')
            OVERWRITE = TRUE;
        ");

        // Verify both are in stage
        $listResult = $connection->fetchAll("LIST @~/$stageName1/;");
        $this->assertNotEmpty($listResult, 'simple table files should be staged');

        $listResult = $connection->fetchAll("LIST @~/$stageName2/;");
        $this->assertNotEmpty($listResult, 'simple-date table files should be staged');

        // Now test the download behavior using SnowsqlExportAdapter
        $logger = new NullLogger();
        $queryFactory = $this->createMock(SnowflakeQueryFactory::class);
        $metadataProvider = $this->createMock(SnowflakeMetadataProvider::class);

        $databaseConfig = SnowflakeDatabaseConfig::fromArray([
            'host' => getenv('SNOWFLAKE_DB_HOST'),
            'port' => getenv('SNOWFLAKE_DB_PORT'),
            'user' => getenv('SNOWFLAKE_DB_USER'),
            '#password' => getenv('SNOWFLAKE_DB_PASSWORD'),
            'database' => getenv('SNOWFLAKE_DB_DATABASE'),
            'schema' => $schema,
            'warehouse' => getenv('SNOWFLAKE_DB_WAREHOUSE'),
        ]);

        // Create ODBC connection using the connection factory
        $connectionFactory = new SnowflakeConnectionFactory($logger, 5);
        $odbcConnection = $connectionFactory->create($databaseConfig);

        $adapter = new SnowsqlExportAdapter(
            $logger,
            $odbcConnection,
            $queryFactory,
            $metadataProvider,
            $databaseConfig,
        );

        // Create export config for downloading "simple" table
        $exportConfig = $this->createMock(ExportConfig::class);
        $exportConfig->method('getOutputTable')->willReturn($stageName1);

        // Use reflection to call generateDownloadSql and execute the download
        $reflection = new ReflectionClass($adapter);
        $method = $reflection->getMethod('generateDownloadSql');
        $method->setAccessible(true);

        $outputDir = $temp->getTmpFolder();
        $downloadCommand = $method->invoke($adapter, $exportConfig, $outputDir);

        // Execute the download command
        assert(is_string($downloadCommand));
        $process = Process::fromShellCommandline($downloadCommand);
        $process->setTimeout(null);
        $process->run();

        $this->assertTrue(
            $process->isSuccessful(),
            'Download process should succeed. Error: ' . $process->getErrorOutput(),
        );

        // Parse the snowsql output to count downloaded files
        $output = $process->getOutput();

        // SnowSQL output format shows a table with DOWNLOADED status entries
        // Count the number of lines with "DOWNLOADED" status (excluding header)
        $downloadedCount = substr_count($output, '| DOWNLOADED |');

        $this->assertEquals(
            1,
            $downloadedCount,
            "Expected 1 file to be downloaded (only from simple/), but got $downloadedCount. " .
            'This indicates the GET command without trailing slash matched multiple prefixes. ' .
            "Output: $output",
        );

        // Verify downloaded files exist
        $downloadedFiles = glob("$outputDir/*");
        $this->assertNotFalse($downloadedFiles, 'glob() should not return false');
        $this->assertCount(
            1,
            $downloadedFiles,
            'Expected exactly 1 file in output directory',
        );

        // Cleanup stage
        $connection->query("REMOVE @~/$stageName1/;");
        $connection->query("REMOVE @~/$stageName2/;");

        // Cleanup tables
        $connection->query('DROP TABLE IF EXISTS "simple";');
        $connection->query('DROP TABLE IF EXISTS "simple-date";');
    }
}
