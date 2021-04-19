<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\CommonExceptions\UserExceptionInterface;
use Keboola\Component\Logger;
use Keboola\DbExtractor\Configuration\ValueObject\SnowflakeDatabaseConfig;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Extractor\Snowflake;
use Keboola\DbExtractor\Extractor\SnowflakeConnectionFactory;
use Keboola\DbExtractor\Extractor\SnowflakeMetadataProvider;
use Keboola\DbExtractor\Extractor\SnowflakeOdbcConnection;
use Keboola\DbExtractor\Extractor\SnowflakeQueryFactory;
use Keboola\DbExtractor\FunctionalTests\TestConnection;
use Keboola\DbExtractor\SnowflakeApplication;
use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\ColumnBuilder;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\Column;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\ColumnCollection;
use Keboola\DbExtractor\Tests\Traits\ConfigTrait;
use Keboola\DbExtractor\TraitTests\RemoveAllTablesTrait;
use Keboola\DbExtractor\TraitTests\Tables\AutoIncrementTableTrait;
use Keboola\DbExtractor\TraitTests\Tables\EscapingTableTrait;
use Keboola\DbExtractor\TraitTests\Tables\SalesTableTrait;
use Keboola\DbExtractor\TraitTests\Tables\TypesTableTrait;
use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Keboola\SnowflakeDbAdapter\Connection;
use Keboola\SnowflakeDbAdapter\QueryBuilder;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class SnowflakeTest extends TestCase
{
    use ConfigTrait;
    use AutoIncrementTableTrait;
    use SalesTableTrait;
    use EscapingTableTrait;
    use TypesTableTrait;
    use RemoveAllTablesTrait;

    protected string $dataDir = __DIR__ . '/data';

    protected Connection $connection;

    protected function setUp(): void
    {
        $this->connection = TestConnection::createConnection();
        $this->removeAllTables();
        parent::setUp();
    }

    public function testDefaultWarehouse(): void
    {
        $this->createSalesTable();
        $this->createEscapingTable();
        $this->createTypesTable();

        $config = $this->getConfig();
        $user = $config['parameters']['db']['user'];
        $warehouse = $config['parameters']['db']['warehouse'];

        $this->setUserDefaultWarehouse($user);

        try {
            // run without warehouse param
            unset($config['parameters']['db']['warehouse']);
            $app = new SnowflakeApplication($config, new Logger(), [], $this->dataDir);

            try {
                $app->run();
                $this->fail('Run extractor without warehouse should fail');
            } catch (UserExceptionInterface $e) {
                $this->assertSame(
                    'Error connecting to DB: ' .
                    'Please configure "warehouse" parameter. User default warehouse is not defined.',
                    $e->getMessage()
                );
            }

            // run with warehouse param
            $config = $this->getConfig();
            $app = new SnowflakeApplication($config, new Logger(), [], $this->dataDir);

            $result = $app->run();
            $this->assertEquals('success', $result['status']);
            $this->assertCount(3, $result['imported']);
        } finally {
            $this->setUserDefaultWarehouse($user, $warehouse);
        }
    }

    public function testCredentialsDefaultWarehouse(): void
    {
        $config = $this->getConfig();
        $config['action'] = 'testConnection';
        unset($config['parameters']['tables']);

        $user = $config['parameters']['db']['user'];
        $warehouse = $config['parameters']['db']['warehouse'];

        try {
            // empty default warehouse, specified in config
            $this->setUserDefaultWarehouse($user, null);

            $app = new SnowflakeApplication($config, new Logger(), [], $this->dataDir);
            $result = $app->run();

            $this->assertArrayHasKey('status', $result);
            $this->assertEquals('success', $result['status']);

            // empty default warehouse and not specified in config
            unset($config['parameters']['db']['warehouse']);
            $app = new SnowflakeApplication($config, new Logger(), [], $this->dataDir);

            try {
                $app->run();
                $this->fail('Test connection without warehouse and default warehouse should fail');
            } catch (UserException $e) {
                $this->assertSame(
                    'Connection failed: \'Error connecting to DB: ' .
                    'Please configure "warehouse" parameter. User default warehouse is not defined.\'',
                    $e->getMessage()
                );
            }

            // bad warehouse
            $config['parameters']['db']['warehouse'] = uniqid('test');
            $app = new SnowflakeApplication($config, new Logger(), [], $this->dataDir);

            try {
                $app->run();
                $this->fail('Test connection with invalid warehouse ID should fail');
            } catch (UserException $e) {
                $this->assertMatchesRegularExpression('/Invalid warehouse/ui', $e->getMessage());
            }
        } finally {
            $this->setUserDefaultWarehouse($user, $warehouse);
        }
    }

    public function testRunEmptyQuery(): void
    {
        $this->createEscapingTable();
        $this->generateEscapingRows();
        $this->createSalesTable();
        $this->generateSalesRows();
        $this->createTypesTable();
        $this->generateTypesRows();

        $outputCsvFolder = $this->dataDir . '/out/tables/in.c-main.escaping.csv';
        $outputManifestFile = $this->dataDir . '/out/tables/in.c-main.escaping.csv.manifest';
        @unlink($outputCsvFolder);
        @unlink($outputManifestFile);

        $config = $this->getConfig();
        $config['parameters']['tables'][1]['query'] = "SELECT * FROM \"escaping\" WHERE \"col1\" = '123'";

        $app = new SnowflakeApplication($config, new Logger(), [], $this->dataDir);
        $result = $app->run();

        $history = $this->connection->fetchAll("
            select 
                QUERY_TEXT, QUERY_TAG, END_TIME 
            from 
                table(information_schema.query_history_by_user()) 
            WHERE 
                query_text='SHOW TABLES IN SCHEMA' 
            order by END_TIME DESC
            LIMIT 1;
        ");

        $this->assertSame(
            sprintf('{"runId":"%s"}', getenv('KBC_RUNID')),
            $history[0]['QUERY_TAG']
        );

        $this->assertEquals('success', $result['status']);
        $this->assertFileDoesNotExist($outputCsvFolder);
        $this->assertFileDoesNotExist($outputManifestFile);
    }

    /**
     * @dataProvider simpleTableColumnsDataProvider
     */
    public function testGetSimplifiedPdoQuery(array $params, array $columnsInfo, array $state, string $expected): void
    {
        $this->createAITable();
        $this->generateAIRows();

        $params['outputTable'] = 'test';
        $params['query'] = '';
        $params['primaryKey'] = [];
        $params['retries'] = 3;
        $exportConfig = ExportConfig::fromArray($params);

        $metadataProvider = $this
            ->getMockBuilder(SnowflakeMetadataProvider::class)
            ->disableOriginalConstructor()
            ->disableAutoReturnValueGeneration()
            ->getMock();
        $odbcConnection = $this
            ->getMockBuilder(SnowflakeOdbcConnection::class)
            ->disableOriginalConstructor()
            ->getMock();
        $odbcConnection
            ->method('quote')
            ->willReturnCallback(function (string $str) {
                return $this->quote($str);
            });
        $odbcConnection
            ->method('quoteIdentifier')
            ->willReturnCallback(function (string $str) {
                return $this->quoteIdentifier($str);
            });
        $columns = array_map(function (array $data): Column {
            $builder = ColumnBuilder::create();
            $builder->setName($data['name']);
            $builder->setType($data['type']);
            return $builder->build();
        }, $columnsInfo);
        $columnCollection = new ColumnCollection($columns);
        $metadataProvider->method('getColumnsInfo')->willReturn($columnCollection);

        $queryFactory = new SnowflakeQueryFactory($metadataProvider, $state);
        if (isset($state['lastFetchedRow']) && is_numeric($state['lastFetchedRow'])) {
            $queryFactory->setIncrementalFetchingColType(Snowflake::INCREMENT_TYPE_NUMERIC);
        }

        $query = $queryFactory->create($exportConfig, $odbcConnection);
        $this->assertEquals($expected, $query);
    }

    public function simpleTableColumnsDataProvider(): array
    {
        return [
            'simple table select with no column metadata' => [
                [
                    'table' => [
                        'tableName' => 'test',
                        'schema' => 'testSchema',
                    ],
                    'columns' => [],
                ],
                [
                    [
                        'name' => 'col1',
                        'type' => 'VARCHAR',
                    ],
                    [
                        'name' => 'col2',
                        'type' => 'VARCHAR',
                    ],
                ],
                [],
                'SELECT * FROM "testSchema"."test"',
            ],
            'simple table with 2 columns selected' => [
                [
                    'table' => [
                        'tableName' => 'test',
                        'schema' => 'testSchema',
                    ],
                    'columns' => [
                        'col1',
                        'col2',
                    ],
                ],
                [
                    [
                        'name' => 'col1',
                        'type' => 'VARCHAR',
                    ],
                    [
                        'name' => 'col2',
                        'type' => 'VARCHAR',
                    ],
                ],
                [],
                'SELECT "col1", "col2" FROM "testSchema"."test"',
            ],
            'test simplePDO query with limit and datetime column but no state' => [
                [
                    'table' => [
                        'tableName' => 'auto Increment Timestamp',
                        'schema' => 'mySchema',
                    ],
                    'columns' => [
                        '_Weir%d I-D',
                        'Weir%d Na-me',
                        'someInteger',
                        'datetime',
                    ],
                    'incrementalFetchingLimit' => 10,
                    'incrementalFetchingColumn' => 'datetime',
                ],
                [
                    [
                        'name' => '_Weir%d I-D',
                        'type' => 'VARCHAR',
                    ],
                    [
                        'name' => 'Weir%d Na-me',
                        'type' => 'VARCHAR',
                    ],
                    [
                        'name' => 'someInteger',
                        'type' => 'INTEGER',
                    ],
                    [
                        'name' => 'datetime',
                        'type' => 'DATETIME',
                    ],
                ],
                [],
                'SELECT "_Weir%d I-D", "Weir%d Na-me", "someInteger", "datetime"' .
                ' FROM "mySchema"."auto Increment Timestamp"' .
                ' ORDER BY "datetime" LIMIT 10',
            ],
            'test simplePDO query with limit and idp column and previos state' => [
                [
                    'table' => [
                        'tableName' => 'auto Increment Timestamp',
                        'schema' => 'mySchema',
                    ],
                    'columns' => [
                        '_Weir%d I-D',
                        'Weir%d Na-me',
                        'someInteger',
                        'datetime',
                    ],
                    'incrementalFetchingLimit' => 10,
                    'incrementalFetchingColumn' => '_Weir%d I-D',
                ],
                [
                    [
                        'name' => '_Weir%d I-D',
                        'type' => 'VARCHAR',
                    ],
                    [
                        'name' => 'Weir%d Na-me',
                        'type' => 'VARCHAR',
                    ],
                    [
                        'name' => 'someInteger',
                        'type' => 'INTEGER',
                    ],
                    [
                        'name' => 'datetime',
                        'type' => 'DATETIME',
                    ],
                ],
                [
                    'lastFetchedRow' => 4,
                ],
                'SELECT "_Weir%d I-D", "Weir%d Na-me", "someInteger", "datetime"' .
                ' FROM "mySchema"."auto Increment Timestamp"' .
                ' WHERE "_Weir%d I-D" >= 4' .
                ' ORDER BY "_Weir%d I-D" LIMIT 10',
            ],
            'test simplePDO query datetime column but no state and no limit' => [
                [
                    'table' => [
                        'tableName' => 'auto Increment Timestamp',
                        'schema' => 'mySchema',
                    ],
                    'columns' => [
                        '_Weir%d I-D',
                        'Weir%d Na-me',
                        'someInteger',
                        'datetime',
                    ],
                    'incrementalFetchingLimit' => null,
                    'incrementalFetchingColumn' => 'datetime',
                ],
                [
                    [
                        'name' => '_Weir%d I-D',
                        'type' => 'VARCHAR',
                    ],
                    [
                        'name' => 'Weir%d Na-me',
                        'type' => 'VARCHAR',
                    ],
                    [
                        'name' => 'someInteger',
                        'type' => 'INTEGER',
                    ],
                    [
                        'name' => 'datetime',
                        'type' => 'DATETIME',
                    ],
                ],
                [],

                'SELECT "_Weir%d I-D", "Weir%d Na-me", "someInteger", "datetime"' .
                ' FROM "mySchema"."auto Increment Timestamp"' .
                ' ORDER BY "datetime"',
            ],
            'test simplePDO query datetime column and previos state and limit' => [
                [
                    'table' => [
                        'tableName' => 'auto Increment Timestamp',
                        'schema' => 'mySchema',
                    ],
                    'columns' => [
                        '_Weir%d I-D',
                        'Weir%d Na-me',
                        'someInteger',
                        'datetime',
                    ],
                    'incrementalFetchingLimit' => 1000,
                    'incrementalFetchingColumn' => 'datetime',
                ],
                [
                    [
                        'name' => '_Weir%d I-D',
                        'type' => 'VARCHAR',
                    ],
                    [
                        'name' => 'Weir%d Na-me',
                        'type' => 'VARCHAR',
                    ],
                    [
                        'name' => 'someInteger',
                        'type' => 'INTEGER',
                    ],
                    [
                        'name' => 'datetime',
                        'type' => 'DATETIME',
                    ],
                ],
                [
                    'lastFetchedRow' => '2018-10-26 10:52:32',
                ],
                'SELECT "_Weir%d I-D", "Weir%d Na-me", "someInteger", "datetime"' .
                ' FROM "mySchema"."auto Increment Timestamp"' .
                ' WHERE "datetime" >= \'2018-10-26 10:52:32\'' .
                ' ORDER BY "datetime"' .
                ' LIMIT 1000',
            ],
            'test semi-structured types select all' => [
                [
                    'table' => [
                        'tableName' => 'auto Increment Timestamp',
                        'schema' => 'mySchema',
                    ],
                    'columns' => [],
                    'incrementalFetchingLimit' => 1000,
                    'incrementalFetchingColumn' => 'id',
                ],
                [
                    [
                        'name' => 'id',
                        'type' => 'INTEGER',
                    ],
                    [
                        'name' => 'name',
                        'type' => 'VARCHAR',
                    ],
                    [
                        'name' => 'col1',
                        'type' => 'VARIANT',
                    ],
                    [
                        'name' => 'col2',
                        'type' => 'OBJECT',
                    ],
                    [
                        'name' => 'col3',
                        'type' => 'ARRAY',
                    ],
                ],
                [
                    'lastFetchedRow' => '123',
                ],
                'SELECT "id", "name", CAST("col1" AS TEXT) AS "col1", '.
                'CAST("col2" AS TEXT) AS "col2", CAST("col3" AS TEXT) AS "col3"' .
                ' FROM "mySchema"."auto Increment Timestamp"' .
                ' WHERE "id" >= 123' .
                ' ORDER BY "id"' .
                ' LIMIT 1000',
            ],
            'test semi-structured types select listed columns' => [
                [
                    'table' => [
                        'tableName' => 'auto Increment Timestamp',
                        'schema' => 'mySchema',
                    ],
                    'columns' => [
                        'id',
                        'name',
                        'col1',
                        'col2',
                        'col3',
                    ],
                    'incrementalFetchingLimit' => 1000,
                    'incrementalFetchingColumn' => 'id',
                ],
                [
                    [
                        'name' => 'id',
                        'type' => 'INTEGER',
                    ],
                    [
                        'name' => 'name',
                        'type' => 'VARCHAR',
                    ],
                    [
                        'name' => 'col1',
                        'type' => 'VARIANT',
                    ],
                    [
                        'name' => 'col2',
                        'type' => 'OBJECT',
                    ],
                    [
                        'name' => 'col3',
                        'type' => 'ARRAY',
                    ],
                ],
                [
                    'lastFetchedRow' => '123',
                ],
                'SELECT "id", "name", CAST("col1" AS TEXT) AS "col1", '.
                'CAST("col2" AS TEXT) AS "col2", CAST("col3" AS TEXT) AS "col3"' .
                ' FROM "mySchema"."auto Increment Timestamp"' .
                ' WHERE "id" >= 123' .
                ' ORDER BY "id"' .
                ' LIMIT 1000',
            ],
        ];
    }

    private function getUserDefaultWarehouse(string $user): ?string
    {
        $sql = sprintf(
            'DESC USER %s;',
            QueryBuilder::quoteIdentifier($user)
        );

        $config = $this->connection->fetchAll($sql);

        foreach ($config as $item) {
            if ($item['property'] === 'DEFAULT_WAREHOUSE') {
                return $item['value'] === 'null' ? null : $item['value'];
            }
        }

        return null;
    }

    private function setUserDefaultWarehouse(string $user, ?string $warehouse = null): void
    {
        if ($warehouse) {
            $sql = sprintf(
                'ALTER USER %s SET DEFAULT_WAREHOUSE = %s;',
                QueryBuilder::quoteIdentifier($user),
                QueryBuilder::quoteIdentifier($warehouse)
            );
            $this->connection->query($sql);

            $this->assertEquals($warehouse, $this->getUserDefaultWarehouse($user));
        } else {
            $sql = sprintf(
                'ALTER USER %s SET DEFAULT_WAREHOUSE = null;',
                QueryBuilder::quoteIdentifier($user)
            );
            $this->connection->query($sql);

            $this->assertEmpty($this->getUserDefaultWarehouse($user));
        }
    }
}
