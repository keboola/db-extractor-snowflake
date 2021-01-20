<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\Component\Logger;
use Keboola\Csv\CsvReader;
use Keboola\DbExtractor\Configuration\ValueObject\SnowflakeExportConfig;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Extractor\Snowflake;
use Keboola\SnowflakeDbAdapter\QueryBuilder;

class SnowflakeTest extends AbstractSnowflakeTest
{
    public function testDefaultWarehouse(): void
    {
        $config = $this->getConfig();
        $user = $config['parameters']['db']['user'];
        $warehouse = $config['parameters']['db']['warehouse'];

        $this->setUserDefaultWarehouse($user);

        // run without warehouse param
        unset($config['parameters']['db']['warehouse']);
        $app = $this->createApplication($config);

        try {
            $app->run();
            $this->fail('Run extractor without warehouse should fail');
        } catch (\Throwable $e) {
            $this->assertMatchesRegularExpression('/No active warehouse/ui', $e->getMessage());
        }

        // run with warehouse param
        $config = $this->getConfig();
        $app = $this->createApplication($config);

        $result = $app->run();
        $this->assertEquals('success', $result['status']);
        $this->assertCount(3, $result['imported']);

        $this->setUserDefaultWarehouse($user, $warehouse);
    }

    public function testCredentials(): void
    {
        $config = $this->getConfig();
        $config['action'] = 'testConnection';
        unset($config['parameters']['tables']);

        $app = $this->createApplication($config);
        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertEquals('success', $result['status']);
    }

    public function testCredentialsWithoutSchema(): void
    {
        $config = $this->getConfig();
        $config['action'] = 'testConnection';
        unset($config['parameters']['tables']);
        unset($config['parameters']['db']['schema']);

        $app = $this->createApplication($config);
        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertEquals('success', $result['status']);
    }

    public function testCredentialsDefaultWarehouse(): void
    {
        $config = $this->getConfig();
        $config['action'] = 'testConnection';
        unset($config['parameters']['tables']);

        $user = $config['parameters']['db']['user'];
        $warehouse = $config['parameters']['db']['warehouse'];

        // empty default warehouse, specified in config
        $this->setUserDefaultWarehouse($user, null);

        $app = $this->createApplication($config);
        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertEquals('success', $result['status']);

        // empty default warehouse and not specified in config
        unset($config['parameters']['db']['warehouse']);
        $app = $this->createApplication($config);

        try {
            $app->run();
            $this->fail('Test connection without warehouse and default warehouse should fail');
        } catch (UserException $e) {
            $this->assertMatchesRegularExpression('/Specify \"warehouse\" parameter/ui', $e->getMessage());
        }

        // bad warehouse
        $config['parameters']['db']['warehouse'] = uniqid('test');
        $app = $this->createApplication($config);

        try {
            $app->run();
            $this->fail('Test connection with invalid warehouse ID should fail');
        } catch (UserException $e) {
            $this->assertMatchesRegularExpression('/Cannot access object or it does not exist/ui', $e->getMessage());
        }

        $this->setUserDefaultWarehouse($user, $warehouse);
    }

    public function testRunWithoutTables(): void
    {
        $config = $this->getConfig();

        unset($config['parameters']['tables']);

        $app = $this->createApplication($config);
        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertEquals('success', $result['status']);
    }

    public function testRunMain(): void
    {
        $this->markTestSkipped();
        $config = $this->getConfig();
        $app = $this->createApplication($config);

        $csvSales = $this->getSalesRows();

        $csvEscaping = $this->getEscapingRows();

        $csvTypes = $this->getTypesRows();

        $result = $app->run();
        $this->assertEquals('success', $result['status']);
        $this->assertCount(3, $result['imported']);

        // remove header
        $outCsv1 = new CsvReader($this->dataDir . '/out/tables/in_c-main_sales.csv.gz/part_0_0_0.csv');
        $this->assertEquals($csvSales['data'], iterator_to_array($outCsv1));
        $this->assertEquals(100, $result['imported']['0']['rows']);

        $outCsv2 = new CsvReader($this->dataDir . '/out/tables/in_c-main_escaping.csv.gz/part_0_0_0.csv');
        $this->assertEquals($csvEscaping['data'], iterator_to_array($outCsv2));
        $this->assertEquals(7, $result['imported']['1']['rows']);

        $outCsv3 = new CsvReader($this->dataDir . '/out/tables/in_c-main_tableColumns.csv.gz/part_0_0_0.csv');
        $this->assertEquals($csvTypes['data'], iterator_to_array($outCsv3));
        $this->assertEquals(4, $result['imported']['2']['rows']);
    }

    public function testRunWithoutSchema(): void
    {
        $config = $this->getConfig();
        unset($config['parameters']['db']['schema']);
        $table = $config['parameters']['tables'][1];
        unset($config['parameters']['tables']);
        $config['parameters']['tables'] = [$table];

        // running the query that doesn't specify schema in the query should produce a user error
        $app = $this->createApplication($config);
        try {
            $result = $app->run();
            $this->fail('The query does not specify schema and no schema is specified in the connection.');
        } catch (\Throwable $e) {
            $this->assertStringContainsString('no schema is specified', $e->getMessage());
        }

        // add schema to db query
        $config['parameters']['tables'][0]['query'] = sprintf(
            'SELECT * FROM %s."escaping"',
            QueryBuilder::quoteIdentifier($this->getEnv('snowflake', 'DB_SCHEMA'))
        );

        $app = $this->createApplication($config);
        $app->run();
        $this->validateExtraction($config['parameters']['tables'][0]);
    }

    public function testRunEmptyQuery(): void
    {
        $outputCsvFolder = $this->dataDir . '/out/tables/in.c-main.escaping.csv';
        $outputManifestFile = $this->dataDir . '/out/tables/in.c-main.escaping.csv.manifest';
        @unlink($outputCsvFolder);
        @unlink($outputManifestFile);

        $config = $this->getConfig();
        $config['parameters']['tables'][1]['query'] = "SELECT * FROM \"escaping\" WHERE \"col1\" = '123'";

        $app = $this->createApplication($config);
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

    public function testGetTablesWithSchema(): void
    {
        $config = $this->getConfig();
        $config['action'] = 'getTables';

        // add a table to a different schema (should not be fetched)
        $this->createEscapingTable('escaping', 'PUBLIC');

        $app = $this->createApplication($config);
        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertArrayHasKey('tables', $result);
        $this->assertEquals('success', $result['status']);
        $this->assertCount(5, $result['tables']);

        $expectedData = array (
            0 =>
                array (
                    'name' => 'escaping',
                    'schema' => $this->getEnv('snowflake', 'DB_SCHEMA'),
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'col1',
                                    'type' => 'TEXT',
                                    'primaryKey' => false,
                                ),
                            1 =>
                                array (
                                    'name' => 'col2',
                                    'type' => 'TEXT',
                                    'primaryKey' => false,
                                ),
                        ),
                ),
            1 =>
                array (
                    'name' => 'sales',
                    'schema' => $this->getEnv('snowflake', 'DB_SCHEMA'),
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'usergender',
                                    'type' => 'TEXT',
                                    'primaryKey' => false,
                                ),
                            1 =>
                                array (
                                    'name' => 'usercity',
                                    'type' => 'TEXT',
                                    'primaryKey' => false,
                                ),
                            2 =>
                                array (
                                    'name' => 'usersentiment',
                                    'type' => 'TEXT',
                                    'primaryKey' => false,
                                ),
                            3 =>
                                array (
                                    'name' => 'zipcode',
                                    'type' => 'TEXT',
                                    'primaryKey' => false,
                                ),
                            4 =>
                                array (
                                    'name' => 'sku',
                                    'type' => 'TEXT',
                                    'primaryKey' => false,
                                ),
                            5 =>
                                array (
                                    'name' => 'createdat',
                                    'type' => 'TEXT',
                                    'primaryKey' => false,
                                ),
                            6 =>
                                array (
                                    'name' => 'category',
                                    'type' => 'TEXT',
                                    'primaryKey' => false,
                                ),
                            7 =>
                                array (
                                    'name' => 'price',
                                    'type' => 'TEXT',
                                    'primaryKey' => false,
                                ),
                            8 =>
                                array (
                                    'name' => 'county',
                                    'type' => 'TEXT',
                                    'primaryKey' => false,
                                ),
                            9 =>
                                array (
                                    'name' => 'countycode',
                                    'type' => 'TEXT',
                                    'primaryKey' => false,
                                ),
                            10 =>
                                array (
                                    'name' => 'userstate',
                                    'type' => 'TEXT',
                                    'primaryKey' => false,
                                ),
                            11 =>
                                array (
                                    'name' => 'categorygroup',
                                    'type' => 'TEXT',
                                    'primaryKey' => false,
                                ),
                        ),
                ),
            2 =>
                array (
                    'name' => 'semi-structured',
                    'schema' => $this->getEnv('snowflake', 'DB_SCHEMA'),
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'var',
                                    'type' => 'VARIANT',
                                    'primaryKey' => false,
                                ),
                            1 =>
                                array (
                                    'name' => 'obj',
                                    'type' => 'OBJECT',
                                    'primaryKey' => false,
                                ),
                            2 =>
                                array (
                                    'name' => 'arr',
                                    'type' => 'ARRAY',
                                    'primaryKey' => false,
                                ),
                        ),
                ),
            3 =>
                array (
                    'name' => 'types',
                    'schema' => $this->getEnv('snowflake', 'DB_SCHEMA'),
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'character',
                                    'type' => 'TEXT',
                                    'primaryKey' => false,
                                ),
                            1 =>
                                array (
                                    'name' => 'integer',
                                    'type' => 'NUMBER',
                                    'primaryKey' => false,
                                ),
                            2 =>
                                array (
                                    'name' => 'decimal',
                                    'type' => 'NUMBER',
                                    'primaryKey' => false,
                                ),
                            3 =>
                                array (
                                    'name' => 'date',
                                    'type' => 'DATE',
                                    'primaryKey' => false,
                                ),
                        ),
                ),
            4 =>
                array (
                    'name' => 'escaping_view',
                    'schema' => $this->getEnv('snowflake', 'DB_SCHEMA'),
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'col1',
                                    'type' => 'TEXT',
                                    'primaryKey' => false,
                                ),
                            1 =>
                                array (
                                    'name' => 'col2',
                                    'type' => 'TEXT',
                                    'primaryKey' => false,
                                ),
                        ),
                ),
        );

        $this->assertEquals($expectedData, $result['tables']);
    }

    public function testGetTablesWithoutSchema(): void
    {
        $config = $this->getConfig();
        $config['action'] = 'getTables';
        unset($config['parameters']['db']['schema']);

        // add a table to a different schema
        $this->createEscapingTable('escaping', 'PUBLIC');

        $app = $this->createApplication($config);
        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertArrayHasKey('tables', $result);
        $this->assertEquals('success', $result['status']);
        $this->assertCount(6, $result['tables']);

        $expectedData = array (
            0 =>
                array (
                    'name' => 'escaping',
                    'schema' => $this->getEnv('snowflake', 'DB_SCHEMA'),
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'col1',
                                    'type' => 'TEXT',
                                    'primaryKey' => false,
                                ),
                            1 =>
                                array (
                                    'name' => 'col2',
                                    'type' => 'TEXT',
                                    'primaryKey' => false,
                                ),
                        ),
                ),
            1 =>
                array (
                    'name' => 'sales',
                    'schema' => $this->getEnv('snowflake', 'DB_SCHEMA'),
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'usergender',
                                    'type' => 'TEXT',
                                    'primaryKey' => false,
                                ),
                            1 =>
                                array (
                                    'name' => 'usercity',
                                    'type' => 'TEXT',
                                    'primaryKey' => false,
                                ),
                            2 =>
                                array (
                                    'name' => 'usersentiment',
                                    'type' => 'TEXT',
                                    'primaryKey' => false,
                                ),
                            3 =>
                                array (
                                    'name' => 'zipcode',
                                    'type' => 'TEXT',
                                    'primaryKey' => false,
                                ),
                            4 =>
                                array (
                                    'name' => 'sku',
                                    'type' => 'TEXT',
                                    'primaryKey' => false,
                                ),
                            5 =>
                                array (
                                    'name' => 'createdat',
                                    'type' => 'TEXT',
                                    'primaryKey' => false,
                                ),
                            6 =>
                                array (
                                    'name' => 'category',
                                    'type' => 'TEXT',
                                    'primaryKey' => false,
                                ),
                            7 =>
                                array (
                                    'name' => 'price',
                                    'type' => 'TEXT',
                                    'primaryKey' => false,
                                ),
                            8 =>
                                array (
                                    'name' => 'county',
                                    'type' => 'TEXT',
                                    'primaryKey' => false,
                                ),
                            9 =>
                                array (
                                    'name' => 'countycode',
                                    'type' => 'TEXT',
                                    'primaryKey' => false,
                                ),
                            10 =>
                                array (
                                    'name' => 'userstate',
                                    'type' => 'TEXT',
                                    'primaryKey' => false,
                                ),
                            11 =>
                                array (
                                    'name' => 'categorygroup',
                                    'type' => 'TEXT',
                                    'primaryKey' => false,
                                ),
                        ),
                ),
            2 =>
                array (
                    'name' => 'semi-structured',
                    'schema' => $this->getEnv('snowflake', 'DB_SCHEMA'),
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'var',
                                    'type' => 'VARIANT',
                                    'primaryKey' => false,
                                ),
                            1 =>
                                array (
                                    'name' => 'obj',
                                    'type' => 'OBJECT',
                                    'primaryKey' => false,
                                ),
                            2 =>
                                array (
                                    'name' => 'arr',
                                    'type' => 'ARRAY',
                                    'primaryKey' => false,
                                ),
                        ),
                ),
            3 =>
                array (
                    'name' => 'types',
                    'schema' => $this->getEnv('snowflake', 'DB_SCHEMA'),
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'character',
                                    'type' => 'TEXT',
                                    'primaryKey' => false,
                                ),
                            1 =>
                                array (
                                    'name' => 'integer',
                                    'type' => 'NUMBER',
                                    'primaryKey' => false,
                                ),
                            2 =>
                                array (
                                    'name' => 'decimal',
                                    'type' => 'NUMBER',
                                    'primaryKey' => false,
                                ),
                            3 =>
                                array (
                                    'name' => 'date',
                                    'type' => 'DATE',
                                    'primaryKey' => false,
                                ),
                        ),
                ),
            4 =>
                array (
                    'name' => 'escaping',
                    'schema' => 'PUBLIC',
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'col1',
                                    'type' => 'TEXT',
                                    'primaryKey' => false,
                                ),
                            1 =>
                                array (
                                    'name' => 'col2',
                                    'type' => 'TEXT',
                                    'primaryKey' => false,
                                ),
                        ),
                ),
            5 =>
                array (
                    'name' => 'escaping_view',
                    'schema' => $this->getEnv('snowflake', 'DB_SCHEMA'),
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'col1',
                                    'type' => 'TEXT',
                                    'primaryKey' => false,
                                ),
                            1 =>
                                array(
                                    'name' => 'col2',
                                    'type' => 'TEXT',
                                    'primaryKey' => false,
                                ),
                        ),
                ),
        );

        $this->assertEquals($expectedData, $result['tables']);
    }

    public function testManifestMetadata(): void
    {
        $config = $this->getConfig();

        // use just 1 table
        unset($config['parameters']['tables'][0]);
        unset($config['parameters']['tables'][1]);

        $app = $this->createApplication($config);
        $app->run();

        $outputManifest = json_decode(
            (string) file_get_contents($this->dataDir . '/out/tables/in.c-main.tablecolumns.csv.gz.manifest'),
            true
        );

        $this->assertArrayHasKey('destination', $outputManifest);
        $this->assertArrayHasKey('incremental', $outputManifest);
        $this->assertArrayHasKey('metadata', $outputManifest);

        $expectedTableMetadata = [
            [
                'key' => 'KBC.name',
                'value' => 'types',
            ],
            [
                'key' => 'KBC.sanitizedName',
                'value' => 'types',
            ],
            [
                'key' => 'KBC.schema',
                'value' => $this->getEnv('snowflake', 'DB_SCHEMA'),
            ],
            [
                'key' => 'KBC.catalog',
                'value' => $this->getEnv('snowflake', 'DB_DATABASE'),
            ],
            [
                'key' => 'KBC.type',
                'value' => 'TABLE',
            ],
            [
                'key' => 'KBC.rowCount',
                'value' => '4',
            ],
        ];
        $this->assertEquals($expectedTableMetadata, $outputManifest['metadata']);

        $this->assertArrayHasKey('column_metadata', $outputManifest);
        $this->assertCount(4, $outputManifest['column_metadata']);

        $expectedColumnMetadata = [
            'character' => [
                [
                    'key' => 'KBC.datatype.type',
                    'value' => 'TEXT',
                ],
                [
                    'key' => 'KBC.datatype.nullable',
                    'value' => false,
                ],
                [
                    'key' => 'KBC.datatype.basetype',
                    'value' => 'STRING',
                ],
                [
                    'key' => 'KBC.datatype.length',
                    'value' => '100',
                ],
                [
                    'key' => 'KBC.datatype.default',
                    'value' => '',
                ],
                [
                    'key' => 'KBC.sourceName',
                    'value' => 'character',
                ],
                [
                    'key' => 'KBC.sanitizedName',
                    'value' => 'character',
                ],
                [
                    'key' => 'KBC.primaryKey',
                    'value' => false,
                ],
                [
                    'key' => 'KBC.uniqueKey',
                    'value' => false,
                ],
                [
                    'key' => 'KBC.ordinalPosition',
                    'value' => '1',
                ],
            ],
            'integer' => [
                [
                    'key' => 'KBC.datatype.type',
                    'value' => 'NUMBER',
                ],
                [
                    'key' => 'KBC.datatype.nullable',
                    'value' => true,
                ],
                [
                    'key' => 'KBC.datatype.basetype',
                    'value' => 'NUMERIC',
                ],
                [
                    'key' => 'KBC.datatype.length',
                    'value' => '6,0',
                ],
                [
                    'key' => 'KBC.datatype.default',
                    'value' => '',
                ],
                [
                    'key' => 'KBC.sourceName',
                    'value' => 'integer',
                ],
                [
                    'key' => 'KBC.sanitizedName',
                    'value' => 'integer',
                ],
                [
                    'key' => 'KBC.primaryKey',
                    'value' => false,
                ],
                [
                    'key' => 'KBC.uniqueKey',
                    'value' => false,
                ],
                [
                    'key' => 'KBC.ordinalPosition',
                    'value' => 2,
                ],
            ],
            'decimal' => [
                [
                    'key' => 'KBC.datatype.type',
                    'value' => 'NUMBER',
                ],
                [
                    'key' => 'KBC.datatype.nullable',
                    'value' => true,
                ],
                [
                    'key' => 'KBC.datatype.basetype',
                    'value' => 'NUMERIC',
                ],
                [
                    'key' => 'KBC.datatype.length',
                    'value' => '10,2',
                ],
                [
                    'key' => 'KBC.datatype.default',
                    'value' => '',
                ],
                [
                    'key' => 'KBC.sourceName',
                    'value' => 'decimal',
                ],
                [
                    'key' => 'KBC.sanitizedName',
                    'value' => 'decimal',
                ],
                [
                    'key' => 'KBC.primaryKey',
                    'value' => false,
                ],
                [
                    'key' => 'KBC.uniqueKey',
                    'value' => false,
                ],
                [
                    'key' => 'KBC.ordinalPosition',
                    'value' => 3,
                ],
            ],
            'date' => [
                [
                    'key' => 'KBC.datatype.type',
                    'value' => 'DATE',
                ],
                [
                    'key' => 'KBC.datatype.nullable',
                    'value' => true,
                ],
                [
                    'key' => 'KBC.datatype.basetype',
                    'value' => 'DATE',
                ],
                [
                    'key' => 'KBC.datatype.default',
                    'value' => '',
                ],
                [
                    'key' => 'KBC.sourceName',
                    'value' => 'date',
                ],
                [
                    'key' => 'KBC.sanitizedName',
                    'value' => 'date',
                ],
                [
                    'key' => 'KBC.primaryKey',
                    'value' => false,
                ],
                [
                    'key' => 'KBC.uniqueKey',
                    'value' => false,
                ],
                [
                    'key' => 'KBC.ordinalPosition',
                    'value' => 4,
                ],
            ],
        ];
        $this->assertEquals($expectedColumnMetadata, $outputManifest['column_metadata']);
    }

    public function testSemiStructured(): void
    {
        $config = $this->getConfig();
        $table = $config['parameters']['tables'][0];
        unset($table['query']);
        $table['table'] = [
            'tableName' => 'semi-structured',
            'schema' => $this->getEnv('snowflake', 'DB_SCHEMA'),
        ];
        $table['outputTable'] = 'in.c-main.semi-structured';
        $table['primaryKey'] = null;
        unset($config['parameters']['tables']);
        $config['parameters']['tables'] = [$table];

        $app = $this->createApplication($config);

        $result = $app->run();
        $this->assertEquals('success', $result['status']);

        // validate the output
        $archiveFile = $this->dataDir . '/out/tables/in.c-main.semi-structured.csv.gz/part_0_0_0.csv.gz';
        exec('gunzip -d ' . escapeshellarg($archiveFile), $output, $return);
        $this->assertEquals(0, $return);

        $rawFile = $this->dataDir . '/out/tables/in.c-main.semi-structured.csv.gz/part_0_0_0.csv';
        $this->assertEquals(
            file_get_contents($this->dataDir . '/snowflake/expected-semi-structured.csv'),
            file_get_contents($rawFile)
        );
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

    /**
     * @dataProvider simpleTableColumnsDataProvider
     */
    public function testGetSimplifiedPdoQuery(array $params, array $state, string $expected): void
    {
        $params['outputTable'] = 'test';
        $params['primaryKey'] = [];
        $params['retries'] = 3;
        $exportConfig = SnowflakeExportConfig::fromArray($params);
        if (isset($params['incrementalFetchingColumn']) && $params['incrementalFetchingColumn'] !== '') {
            $incrementalConfig = $this->getIncrementalConfig();
            $this->createAutoIncrementAndTimestampTable($incrementalConfig);
            $extractor = new Snowflake($incrementalConfig['parameters'], $state, new Logger());
            $extractor->validateIncrementalFetching($exportConfig);
        } else {
            $config = $this->getConfig();
            $extractor = new Snowflake($config['parameters'], $state, new Logger());
        }
        $query = $extractor->simpleQuery($exportConfig);
        $this->assertEquals($expected, $query);
    }

    public function simpleTableColumnsDataProvider(): array
    {
        $dbSchema = $this->getEnv(self::DRIVER, 'DB_SCHEMA');
        return [
            'simple table select with no column metadata' => [
                [
                    'table' => [
                        'tableName' => 'test',
                        'schema' => 'testSchema',
                    ],
                    'columns' => [],
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
                [],
                'SELECT "col1", "col2" FROM "testSchema"."test"',
            ],
            'test simplePDO query with limit and datetime column but no state' => [
                [
                    'table' => [
                        'tableName' => 'auto_increment_timestamp',
                        'schema' => $dbSchema,
                    ],
                    'columns' => [
                        'id',
                        'name',
                        'number',
                        'timestamp',
                    ],
                    'incrementalFetchingLimit' => 10,
                    'incrementalFetchingColumn' => 'timestamp',
                ],
                [],
                sprintf(
                    'SELECT "id", "name", "number", "timestamp"' .
                    ' FROM "%s"."auto_increment_timestamp"' .
                    ' ORDER BY "timestamp" LIMIT 10',
                    $dbSchema
                ),
            ],
            'test simplePDO query with limit and idp column and previos state' => [
                [
                    'table' => [
                        'tableName' => 'auto_increment_timestamp',
                        'schema' => $dbSchema,
                    ],
                    'columns' => [
                        'id',
                        'name',
                        'number',
                        'timestamp',
                    ],
                    'incrementalFetchingLimit' => 10,
                    'incrementalFetchingColumn' => 'id',
                ],
                [
                    'lastFetchedRow' => 4,
                ],
                sprintf(
                    'SELECT "id", "name", "number", "timestamp"' .
                    ' FROM "%s"."auto_increment_timestamp"' .
                    ' WHERE "id" >= 4' .
                    ' ORDER BY "id" LIMIT 10',
                    $dbSchema
                ),
            ],
            'test simplePDO query datetime column but no state and no limit' => [
                [
                    'table' => [
                        'tableName' => 'auto_increment_timestamp',
                        'schema' => $dbSchema,
                    ],
                    'columns' => [
                        'id',
                        'name',
                        'number',
                        'timestamp',
                    ],
                    'incrementalFetchingLimit' => null,
                    'incrementalFetchingColumn' => 'timestamp',
                ],
                [],
                sprintf(
                    'SELECT "id", "name", "number", "timestamp"' .
                    ' FROM "%s"."auto_increment_timestamp"' .
                    ' ORDER BY "timestamp"',
                    $dbSchema
                ),
            ],
            'test simplePDO query id column and previos state and no limit' => [
                [
                    'table' => [
                        'tableName' => 'auto_increment_timestamp',
                        'schema' => $dbSchema,
                    ],
                    'columns' => [
                        'id',
                        'name',
                        'number',
                        'timestamp',
                    ],
                    'incrementalFetchingLimit' => 0,
                    'incrementalFetchingColumn' => 'id',
                ],
                [
                    'lastFetchedRow' => 4,
                ],
                sprintf(
                    'SELECT "id", "name", "number", "timestamp"' .
                    ' FROM "%s"."auto_increment_timestamp"' .
                    ' WHERE "id" >= 4' .
                    ' ORDER BY "id"',
                    $dbSchema
                ),
            ],
            'test simplePDO query datetime column and previos state and limit' => [
                [
                    'table' => [
                        'tableName' => 'auto_increment_timestamp',
                        'schema' => $dbSchema,
                    ],
                    'columns' => [
                        'id',
                        'name',
                        'number',
                        'timestamp',
                    ],
                    'incrementalFetchingLimit' => 1000,
                    'incrementalFetchingColumn' => 'timestamp',
                ],
                [
                    'lastFetchedRow' => '2018-10-26 10:52:32',
                ],
                sprintf(
                    'SELECT "id", "name", "number", "timestamp"' .
                    ' FROM "%s"."auto_increment_timestamp"' .
                    ' WHERE "timestamp" >= \'2018-10-26 10:52:32\'' .
                    ' ORDER BY "timestamp"' .
                    ' LIMIT 1000',
                    $dbSchema
                ),
            ],
        ];
    }

    private function validateExtraction(array $query, int $expectedFiles = 1): void
    {

        $dirPath = $this->dataDir . '/out/tables';
        $outputTable = $query['outputTable'];

        $manifestFiles = array_map(
            function ($manifestFileName) use ($dirPath) {
                return $dirPath . '/' . $manifestFileName;
            },
            array_filter(
                (array) scandir($dirPath),
                function ($fileName) use ($dirPath, $outputTable) {
                    $filePath = $dirPath . '/' . $fileName;
                    if (is_dir($filePath)) {
                        return false;
                    }

                    $file = new \SplFileInfo($filePath);
                    if ($file->getExtension() !== 'manifest') {
                        return false;
                    }

                    $manifest = json_decode((string) file_get_contents($file->getPathname()), true);
                    return $manifest['destination'] === $outputTable;
                }
            )
        );

        if (!$expectedFiles) {
            return;
        }

        $this->assertCount($expectedFiles, $manifestFiles);
        foreach ($manifestFiles as $file) {
            // manifest validation
            $params = json_decode((string) file_get_contents($file), true);

            $this->assertArrayHasKey('destination', $params);
            $this->assertArrayHasKey('incremental', $params);

            if ($query['primaryKey']) {
                $this->assertEquals($query['primaryKey'], $params['primary_key']);
            } else {
                $this->assertEmpty($params['primary_key']);
            }

            $this->assertEquals($query['incremental'], $params['incremental']);

            if (isset($query['outputTable'])) {
                $this->assertEquals($query['outputTable'], $params['destination']);
            }

            $csvDir = str_replace('.manifest', '', $file);

            $this->assertTrue(is_dir($csvDir));

            foreach (array_diff((array) scandir($csvDir), array('..', '.')) as $csvFile) {
                // archive validation
                $archiveFile = $csvDir . '/' . $csvFile;
                $pos = strrpos($archiveFile, '.gz');
                $rawFile = new \SplFileInfo(substr_replace($archiveFile, '', $pos, strlen('.gz')));

                clearstatcache();
                $this->assertFalse($rawFile->isFile());

                exec('gunzip -d ' . escapeshellarg($archiveFile), $output, $return);
                $this->assertEquals(0, $return);

                clearstatcache();
                $this->assertTrue($rawFile->isFile());
            }
        }
    }
}