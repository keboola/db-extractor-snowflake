<?php

declare(strict_types=1);

namespace Keboola\Test;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Exception\UserException;
use Symfony\Component\Yaml\Yaml;

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
            $this->assertRegExp('/No active warehouse/ui', $e->getMessage());
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
            $this->assertRegExp('/Specify \"warehouse\" parameter/ui', $e->getMessage());
        }

        // bad warehouse
        $config['parameters']['db']['warehouse'] = uniqid('test');
        $app = $this->createApplication($config);

        try {
            $app->run();
            $this->fail('Test connection with invalid warehouse ID should fail');
        } catch (UserException $e) {
            $this->assertRegExp('/Invalid warehouse/ui', $e->getMessage());
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
        $config = $this->getConfig();
        $app = $this->createApplication($config);

        $csv1 = new CsvFile($this->dataDir . '/snowflake/sales.csv');
        $this->createTextTable($csv1);
        $header1 = $csv1->getHeader();

        $csv2 = new CsvFile($this->dataDir . '/snowflake/escaping.csv');
        $this->createTextTable($csv2);
        $header2 = $csv2->getHeader();

        $csv3 = new CsvFile($this->dataDir . '/snowflake/types.csv');
        $header3 = $csv3->getHeader();

        $result = $app->run();
        $this->assertEquals('success', $result['status']);
        $this->assertCount(3, $result['imported']);

        $columns = [];
        foreach ($config['parameters']['tables'] as $table) {
            $columns[] = $this->validateExtraction($table, $table['enabled'] ? 1 : 0);
        }

        // validate columns with file header
        $this->assertEquals($header1, $columns[0]);
        $this->assertEquals($header2, $columns[1]);
        $this->assertEquals($header3, $columns[2]);

        // remove header
        $csv1arr = iterator_to_array($csv1);
        array_shift($csv1arr);
        $outCsv1 = new CsvFile($this->dataDir . '/out/tables/in_c-main_sales.csv.gz/part_0_0_0.csv');
        $this->assertEquals($csv1arr, iterator_to_array($outCsv1));
        $this->assertEquals(100, $result['imported']['0']['rows']);

        $csv2arr = iterator_to_array($csv2);
        array_shift($csv2arr);
        $outCsv2 = new CsvFile($this->dataDir . '/out/tables/in_c-main_escaping.csv.gz/part_0_0_0.csv');
        $this->assertEquals($csv2arr, iterator_to_array($outCsv2));
        $this->assertEquals(7, $result['imported']['1']['rows']);

        $csv3arr = iterator_to_array($csv3);
        array_shift($csv3arr);
        $outCsv3 = new CsvFile($this->dataDir . '/out/tables/in_c-main_tableColumns.csv.gz/part_0_0_0.csv');
        $this->assertEquals($csv3arr, iterator_to_array($outCsv3));
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
            $this->assertContains('no schema is specified', $e->getMessage());
        }

        // add schema to db query
        $config['parameters']['tables'][0]['query'] = sprintf(
            'SELECT * FROM %s."escaping"',
            $this->connection->quoteIdentifier($this->getEnv('snowflake', 'DB_SCHEMA'))
        );

        $app = $this->createApplication($config);
        $app->run();
        $this->validateExtraction($config['parameters']['tables'][0]);
    }

    public function testRunEmptyQuery(): void
    {
        $csv = new CsvFile($this->dataDir . '/snowflake/escaping.csv');
        $this->createTextTable($csv);

        $outputCsvFolder = $this->dataDir . '/out/tables/in.c-main.escaping.csv';
        $outputManifestFile = $this->dataDir . '/out/tables/in.c-main.escaping.csv.manifest';
        @unlink($outputCsvFolder);
        @unlink($outputManifestFile);

        $config = $this->getConfig();
        $config['parameters']['tables'][1]['query'] = "SELECT * FROM \"escaping\" WHERE \"col1\" = '123'";

        $app = $this->createApplication($config);
        $result = $app->run();

        $this->assertEquals('success', $result['status']);
        $this->assertFileNotExists($outputCsvFolder);
        $this->assertFileNotExists($outputManifestFile);
    }

    public function testGetTables(): void
    {
        $config = $this->getConfig();
        $config['action'] = 'getTables';

        // add a table to a different schema (should not be fetched)
        $this->createTextTable(
            new CsvFile($this->dataDir . '/snowflake/escaping.csv'),
            "no_schema_escaping",
            "PUBLIC"
        );

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
                    'catalog' => $this->getEnv('snowflake', 'DB_DATABASE'),
                    'schema' => $this->getEnv('snowflake', 'DB_SCHEMA'),
                    'type' => 'TABLE',
                    'rowCount' => '7',
                    'byteCount' => '1024',
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'col1',
                                    'default' => null,
                                    'length' => '200',
                                    'nullable' => false,
                                    'type' => 'TEXT',
                                    'ordinalPosition' => '1',
                                ),
                            1 =>
                                array (
                                    'name' => 'col2',
                                    'default' => null,
                                    'length' => '200',
                                    'nullable' => false,
                                    'type' => 'TEXT',
                                    'ordinalPosition' => '2',
                                ),
                        ),
                ),
            1 =>
                array (
                    'name' => 'sales',
                    'catalog' => $this->getEnv('snowflake', 'DB_DATABASE'),
                    'schema' => $this->getEnv('snowflake', 'DB_SCHEMA'),
                    'type' => 'TABLE',
                    'rowCount' => '100',
                    'byteCount' => '6656',
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'usergender',
                                    'default' => null,
                                    'length' => '200',
                                    'nullable' => false,
                                    'type' => 'TEXT',
                                    'ordinalPosition' => '1',
                                ),
                            1 =>
                                array (
                                    'name' => 'usercity',
                                    'default' => null,
                                    'length' => '200',
                                    'nullable' => false,
                                    'type' => 'TEXT',
                                    'ordinalPosition' => '2',
                                ),
                            2 =>
                                array (
                                    'name' => 'usersentiment',
                                    'default' => null,
                                    'length' => '200',
                                    'nullable' => false,
                                    'type' => 'TEXT',
                                    'ordinalPosition' => '3',
                                ),
                            3 =>
                                array (
                                    'name' => 'zipcode',
                                    'default' => null,
                                    'length' => '200',
                                    'nullable' => false,
                                    'type' => 'TEXT',
                                    'ordinalPosition' => '4',
                                ),
                            4 =>
                                array (
                                    'name' => 'sku',
                                    'default' => null,
                                    'length' => '200',
                                    'nullable' => false,
                                    'type' => 'TEXT',
                                    'ordinalPosition' => '5',
                                ),
                            5 =>
                                array (
                                    'name' => 'createdat',
                                    'default' => null,
                                    'length' => '200',
                                    'nullable' => false,
                                    'type' => 'TEXT',
                                    'ordinalPosition' => '6',
                                ),
                            6 =>
                                array (
                                    'name' => 'category',
                                    'default' => null,
                                    'length' => '200',
                                    'nullable' => false,
                                    'type' => 'TEXT',
                                    'ordinalPosition' => '7',
                                ),
                            7 =>
                                array (
                                    'name' => 'price',
                                    'default' => null,
                                    'length' => '200',
                                    'nullable' => false,
                                    'type' => 'TEXT',
                                    'ordinalPosition' => '8',
                                ),
                            8 =>
                                array (
                                    'name' => 'county',
                                    'default' => null,
                                    'length' => '200',
                                    'nullable' => false,
                                    'type' => 'TEXT',
                                    'ordinalPosition' => '9',
                                ),
                            9 =>
                                array (
                                    'name' => 'countycode',
                                    'default' => null,
                                    'length' => '200',
                                    'nullable' => false,
                                    'type' => 'TEXT',
                                    'ordinalPosition' => '10',
                                ),
                            10 =>
                                array (
                                    'name' => 'userstate',
                                    'default' => null,
                                    'length' => '200',
                                    'nullable' => false,
                                    'type' => 'TEXT',
                                    'ordinalPosition' => '11',
                                ),
                            11 =>
                                array (
                                    'name' => 'categorygroup',
                                    'default' => null,
                                    'length' => '200',
                                    'nullable' => false,
                                    'type' => 'TEXT',
                                    'ordinalPosition' => '12',
                                ),
                        ),
                ),
            2 =>
                array (
                    'name' => 'semi-structured',
                    'catalog' => $this->getEnv('snowflake', 'DB_DATABASE'),
                    'schema' => $this->getEnv('snowflake', 'DB_SCHEMA'),
                    'type' => 'TABLE',
                    'rowCount' => '1',
                    'byteCount' => '1024',
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'var',
                                    'default' => null,
                                    'length' => null,
                                    'nullable' => true,
                                    'type' => 'VARIANT',
                                    'ordinalPosition' => '1',
                                ),
                            1 =>
                                array (
                                    'name' => 'obj',
                                    'default' => null,
                                    'length' => null,
                                    'nullable' => true,
                                    'type' => 'OBJECT',
                                    'ordinalPosition' => '2',
                                ),
                            2 =>
                                array (
                                    'name' => 'arr',
                                    'default' => null,
                                    'length' => null,
                                    'nullable' => true,
                                    'type' => 'ARRAY',
                                    'ordinalPosition' => '3',
                                ),
                        ),
                ),
            3 =>
                array (
                    'name' => 'types',
                    'catalog' => $this->getEnv('snowflake', 'DB_DATABASE'),
                    'schema' => $this->getEnv('snowflake', 'DB_SCHEMA'),
                    'type' => 'TABLE',
                    'rowCount' => '4',
                    'byteCount' => '1024',
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'character',
                                    'default' => null,
                                    'length' => '100',
                                    'nullable' => false,
                                    'type' => 'TEXT',
                                    'ordinalPosition' => '1',
                                ),
                            1 =>
                                array (
                                    'name' => 'integer',
                                    'default' => null,
                                    'length' => '6,0',
                                    'nullable' => true,
                                    'type' => 'NUMBER',
                                    'ordinalPosition' => '2',
                                ),
                            2 =>
                                array (
                                    'name' => 'decimal',
                                    'default' => null,
                                    'length' => '10,2',
                                    'nullable' => true,
                                    'type' => 'NUMBER',
                                    'ordinalPosition' => '3',
                                ),
                            3 =>
                                array (
                                    'name' => 'date',
                                    'default' => null,
                                    'length' => null,
                                    'nullable' => true,
                                    'type' => 'DATE',
                                    'ordinalPosition' => '4',
                                ),
                        ),
                ),
            4 =>
                array (
                    'name' => 'escaping_view',
                    'catalog' => $this->getEnv('snowflake', 'DB_DATABASE'),
                    'schema' => $this->getEnv('snowflake', 'DB_SCHEMA'),
                    'type' => 'VIEW',
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'col1',
                                    'default' => null,
                                    'length' => '200',
                                    'nullable' => true,
                                    'type' => 'TEXT',
                                    'ordinalPosition' => '1',
                                ),
                            1 =>
                                array (
                                    'name' => 'col2',
                                    'default' => null,
                                    'length' => '200',
                                    'nullable' => true,
                                    'type' => 'TEXT',
                                    'ordinalPosition' => '2',
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

        // add a table to a different schema (should not be fetched)
        $this->createTextTable(
            new CsvFile($this->dataDir . '/snowflake/escaping.csv'),
            "no_schema_escaping",
            "PUBLIC"
        );

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
                    'catalog' => $this->getEnv('snowflake', 'DB_DATABASE'),
                    'schema' => $this->getEnv('snowflake', 'DB_SCHEMA'),
                    'type' => 'TABLE',
                    'rowCount' => '7',
                    'byteCount' => '1024',
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'col1',
                                    'default' => null,
                                    'length' => '200',
                                    'nullable' => false,
                                    'type' => 'TEXT',
                                    'ordinalPosition' => '1',
                                ),
                            1 =>
                                array (
                                    'name' => 'col2',
                                    'default' => null,
                                    'length' => '200',
                                    'nullable' => false,
                                    'type' => 'TEXT',
                                    'ordinalPosition' => '2',
                                ),
                        ),
                ),
            1 =>
                array (
                    'name' => 'sales',
                    'catalog' => $this->getEnv('snowflake', 'DB_DATABASE'),
                    'schema' => $this->getEnv('snowflake', 'DB_SCHEMA'),
                    'type' => 'TABLE',
                    'rowCount' => '100',
                    'byteCount' => '6656',
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'usergender',
                                    'default' => null,
                                    'length' => '200',
                                    'nullable' => false,
                                    'type' => 'TEXT',
                                    'ordinalPosition' => '1',
                                ),
                            1 =>
                                array (
                                    'name' => 'usercity',
                                    'default' => null,
                                    'length' => '200',
                                    'nullable' => false,
                                    'type' => 'TEXT',
                                    'ordinalPosition' => '2',
                                ),
                            2 =>
                                array (
                                    'name' => 'usersentiment',
                                    'default' => null,
                                    'length' => '200',
                                    'nullable' => false,
                                    'type' => 'TEXT',
                                    'ordinalPosition' => '3',
                                ),
                            3 =>
                                array (
                                    'name' => 'zipcode',
                                    'default' => null,
                                    'length' => '200',
                                    'nullable' => false,
                                    'type' => 'TEXT',
                                    'ordinalPosition' => '4',
                                ),
                            4 =>
                                array (
                                    'name' => 'sku',
                                    'default' => null,
                                    'length' => '200',
                                    'nullable' => false,
                                    'type' => 'TEXT',
                                    'ordinalPosition' => '5',
                                ),
                            5 =>
                                array (
                                    'name' => 'createdat',
                                    'default' => null,
                                    'length' => '200',
                                    'nullable' => false,
                                    'type' => 'TEXT',
                                    'ordinalPosition' => '6',
                                ),
                            6 =>
                                array (
                                    'name' => 'category',
                                    'default' => null,
                                    'length' => '200',
                                    'nullable' => false,
                                    'type' => 'TEXT',
                                    'ordinalPosition' => '7',
                                ),
                            7 =>
                                array (
                                    'name' => 'price',
                                    'default' => null,
                                    'length' => '200',
                                    'nullable' => false,
                                    'type' => 'TEXT',
                                    'ordinalPosition' => '8',
                                ),
                            8 =>
                                array (
                                    'name' => 'county',
                                    'default' => null,
                                    'length' => '200',
                                    'nullable' => false,
                                    'type' => 'TEXT',
                                    'ordinalPosition' => '9',
                                ),
                            9 =>
                                array (
                                    'name' => 'countycode',
                                    'default' => null,
                                    'length' => '200',
                                    'nullable' => false,
                                    'type' => 'TEXT',
                                    'ordinalPosition' => '10',
                                ),
                            10 =>
                                array (
                                    'name' => 'userstate',
                                    'default' => null,
                                    'length' => '200',
                                    'nullable' => false,
                                    'type' => 'TEXT',
                                    'ordinalPosition' => '11',
                                ),
                            11 =>
                                array (
                                    'name' => 'categorygroup',
                                    'default' => null,
                                    'length' => '200',
                                    'nullable' => false,
                                    'type' => 'TEXT',
                                    'ordinalPosition' => '12',
                                ),
                        ),
                ),
            2 =>
                array (
                    'name' => 'semi-structured',
                    'catalog' => $this->getEnv('snowflake', 'DB_DATABASE'),
                    'schema' => $this->getEnv('snowflake', 'DB_SCHEMA'),
                    'type' => 'TABLE',
                    'rowCount' => '1',
                    'byteCount' => '1024',
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'var',
                                    'default' => null,
                                    'length' => null,
                                    'nullable' => true,
                                    'type' => 'VARIANT',
                                    'ordinalPosition' => '1',
                                ),
                            1 =>
                                array (
                                    'name' => 'obj',
                                    'default' => null,
                                    'length' => null,
                                    'nullable' => true,
                                    'type' => 'OBJECT',
                                    'ordinalPosition' => '2',
                                ),
                            2 =>
                                array (
                                    'name' => 'arr',
                                    'default' => null,
                                    'length' => null,
                                    'nullable' => true,
                                    'type' => 'ARRAY',
                                    'ordinalPosition' => '3',
                                ),
                        ),
                ),
            3 =>
                array (
                    'name' => 'types',
                    'catalog' => $this->getEnv('snowflake', 'DB_DATABASE'),
                    'schema' => $this->getEnv('snowflake', 'DB_SCHEMA'),
                    'type' => 'TABLE',
                    'rowCount' => '4',
                    'byteCount' => '1024',
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'character',
                                    'default' => null,
                                    'length' => '100',
                                    'nullable' => false,
                                    'type' => 'TEXT',
                                    'ordinalPosition' => '1',
                                ),
                            1 =>
                                array (
                                    'name' => 'integer',
                                    'default' => null,
                                    'length' => '6,0',
                                    'nullable' => true,
                                    'type' => 'NUMBER',
                                    'ordinalPosition' => '2',
                                ),
                            2 =>
                                array (
                                    'name' => 'decimal',
                                    'default' => null,
                                    'length' => '10,2',
                                    'nullable' => true,
                                    'type' => 'NUMBER',
                                    'ordinalPosition' => '3',
                                ),
                            3 =>
                                array (
                                    'name' => 'date',
                                    'default' => null,
                                    'length' => null,
                                    'nullable' => true,
                                    'type' => 'DATE',
                                    'ordinalPosition' => '4',
                                ),
                        ),
                ),
            4 =>
                array (
                    'name' => 'no_schema_escaping',
                    'catalog' => $this->getEnv('snowflake', 'DB_DATABASE'),
                    'schema' => 'PUBLIC',
                    'type' => 'TABLE',
                    'rowCount' => '7',
                    'byteCount' => '1024',
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'col1',
                                    'default' => null,
                                    'length' => '200',
                                    'nullable' => false,
                                    'type' => 'TEXT',
                                    'ordinalPosition' => '1',
                                ),
                            1 =>
                                array (
                                    'name' => 'col2',
                                    'default' => null,
                                    'length' => '200',
                                    'nullable' => false,
                                    'type' => 'TEXT',
                                    'ordinalPosition' => '2',
                                ),
                        ),
                ),
            5 =>
                array (
                    'name' => 'escaping_view',
                    'catalog' => $this->getEnv('snowflake', 'DB_DATABASE'),
                    'schema' => $this->getEnv('snowflake', 'DB_SCHEMA'),
                    'type' => 'VIEW',
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'col1',
                                    'default' => null,
                                    'length' => '200',
                                    'nullable' => true,
                                    'type' => 'TEXT',
                                    'ordinalPosition' => '1',
                                ),
                            1 =>
                                array (
                                    'name' => 'col2',
                                    'default' => null,
                                    'length' => '200',
                                    'nullable' => true,
                                    'type' => 'TEXT',
                                    'ordinalPosition' => '2',
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

        $result = $app->run();

        $outputManifest = Yaml::parse(
            file_get_contents($this->dataDir . '/out/tables/in_c-main_tableColumns.csv.gz.manifest')
        );

        $this->assertArrayHasKey('destination', $outputManifest);
        $this->assertArrayHasKey('incremental', $outputManifest);
        $this->assertArrayHasKey('metadata', $outputManifest);

        $expectedTableMetadata = array (
            0 =>
                array (
                    'key' => 'KBC.name',
                    'value' => 'types',
                ),
            1 =>
                array (
                    'key' => 'KBC.catalog',
                    'value' => $this->getEnv('snowflake', 'DB_DATABASE'),
                ),
            2 =>
                array (
                    'key' => 'KBC.schema',
                    'value' => $this->getEnv('snowflake', 'DB_SCHEMA'),
                ),
            3 =>
                array (
                    'key' => 'KBC.type',
                    'value' => 'TABLE',
                ),
            4 =>
                array (
                    'key' => 'KBC.rowCount',
                    'value' => '4',
                ),
            5 =>
                array (
                    'key' => 'KBC.byteCount',
                    'value' => '1024',
                ),
        );
        $this->assertEquals($expectedTableMetadata, $outputManifest['metadata']);

        $this->assertArrayHasKey('column_metadata', $outputManifest);
        $this->assertCount(4, $outputManifest['column_metadata']);

        $expectedColumnMetadata = array (
            'character' =>
                array (
                    0 =>
                        array (
                            'key' => 'KBC.datatype.type',
                            'value' => 'TEXT',
                        ),
                    1 =>
                        array (
                            'key' => 'KBC.datatype.nullable',
                            'value' => false,
                        ),
                    2 =>
                        array (
                            'key' => 'KBC.datatype.basetype',
                            'value' => 'STRING',
                        ),
                    3 =>
                        array (
                            'key' => 'KBC.datatype.length',
                            'value' => '100',
                        ),
                    4 =>
                        array (
                            'key' => 'KBC.type',
                            'value' => 'TEXT',
                        ),
                    5 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => '1',
                        ),
                ),
            'integer' =>
                array (
                    0 =>
                        array (
                            'key' => 'KBC.datatype.type',
                            'value' => 'NUMBER',
                        ),
                    1 =>
                        array (
                            'key' => 'KBC.datatype.nullable',
                            'value' => true,
                        ),
                    2 =>
                        array (
                            'key' => 'KBC.datatype.basetype',
                            'value' => 'NUMERIC',
                        ),
                    3 =>
                        array (
                            'key' => 'KBC.datatype.length',
                            'value' => '6,0',
                        ),
                    4 =>
                        array (
                            'key' => 'KBC.type',
                            'value' => 'NUMBER',
                        ),
                    5 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => '2',
                        ),
                ),
            'decimal' =>
                array (
                    0 =>
                        array (
                            'key' => 'KBC.datatype.type',
                            'value' => 'NUMBER',
                        ),
                    1 =>
                        array (
                            'key' => 'KBC.datatype.nullable',
                            'value' => true,
                        ),
                    2 =>
                        array (
                            'key' => 'KBC.datatype.basetype',
                            'value' => 'NUMERIC',
                        ),
                    3 =>
                        array (
                            'key' => 'KBC.datatype.length',
                            'value' => '10,2',
                        ),
                    4 =>
                        array (
                            'key' => 'KBC.type',
                            'value' => 'NUMBER',
                        ),
                    5 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => '3',
                        ),
                ),
            'date' =>
                array (
                    0 =>
                        array (
                            'key' => 'KBC.datatype.type',
                            'value' => 'DATE',
                        ),
                    1 =>
                        array (
                            'key' => 'KBC.datatype.nullable',
                            'value' => true,
                        ),
                    2 =>
                        array (
                            'key' => 'KBC.datatype.basetype',
                            'value' => 'DATE',
                        ),
                    3 =>
                        array (
                            'key' => 'KBC.type',
                            'value' => 'DATE',
                        ),
                    4 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => '4',
                        ),
                ),
        );
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
        $archiveFile = $this->dataDir . '/out/tables/in_c-main_semi-structured.csv.gz/part_0_0_0.csv.gz';
        exec("gunzip -d " . escapeshellarg($archiveFile), $output, $return);
        $this->assertEquals(0, $return);

        $rawFile = new \SplFileInfo($this->dataDir . '/out/tables/in_c-main_semi-structured.csv.gz/part_0_0_0.csv');
        $this->assertEquals(
            file_get_contents($this->dataDir . '/snowflake/expected-semi-structured.csv'),
            file_get_contents($rawFile)
        );
    }

    private function getUserDefaultWarehouse(string $user): ?string
    {
        $sql = sprintf(
            "DESC USER %s;",
            $this->connection->quoteIdentifier($user)
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
                "ALTER USER %s SET DEFAULT_WAREHOUSE = %s;",
                $this->connection->quoteIdentifier($user),
                $this->connection->quoteIdentifier($warehouse)
            );
            $this->connection->query($sql);

            $this->assertEquals($warehouse, $this->getUserDefaultWarehouse($user));
        } else {
            $sql = sprintf(
                "ALTER USER %s SET DEFAULT_WAREHOUSE = null;",
                $this->connection->quoteIdentifier($user)
            );
            $this->connection->query($sql);

            $this->assertEmpty($this->getUserDefaultWarehouse($user));
        }
    }

    private function validateExtraction(array $query, int $expectedFiles = 1): ?array
    {

        $dirPath = $this->dataDir . '/out/tables';
        $outputTable = $query['outputTable'];

        $manifestFiles = array_map(
            function ($manifestFileName) use ($dirPath) {
                return $dirPath . '/' . $manifestFileName;
            },
            array_filter(
                scandir($dirPath),
                function ($fileName) use ($dirPath, $outputTable) {
                    $filePath = $dirPath . '/' . $fileName;
                    if (is_dir($filePath)) {
                        return false;
                    }

                    $file = new \SplFileInfo($filePath);
                    if ($file->getExtension() !== 'manifest') {
                        return false;
                    }

                    $manifest = Yaml::parse(file_get_contents($file));
                    return $manifest['destination'] === $outputTable;
                }
            )
        );

        if (!$expectedFiles) {
            return null;
        }

        $this->assertCount($expectedFiles, $manifestFiles);
        $columns = [];
        foreach ($manifestFiles as $file) {
            // manifest validation
            $params = Yaml::parse(file_get_contents($file));

            $this->assertArrayHasKey('destination', $params);
            $this->assertArrayHasKey('incremental', $params);
            $this->assertArrayHasKey('primary_key', $params);
            $this->assertArrayHasKey('columns', $params);
            $columns = $params['columns'];

            if ($query['primaryKey']) {
                $this->assertEquals($query['primaryKey'], $params['primary_key']);
            } else {
                $this->assertEmpty($params['primary_key']);
            }

            $this->assertEquals($query['incremental'], $params['incremental']);

            if (isset($query['outputTable'])) {
                $this->assertEquals($query['outputTable'], $params['destination']);
            }

            $csvDir = new \SplFileInfo(str_replace('.manifest', '', $file));

            $this->assertTrue(is_dir($csvDir));

            foreach (array_diff(scandir($csvDir), array('..', '.')) as $csvFile) {
                // archive validation
                $archiveFile = new \SplFileInfo($csvDir . "/" . $csvFile);
                $pos = strrpos($archiveFile, ".gz");
                $rawFile = new \SplFileInfo(substr_replace($archiveFile, '', $pos, strlen(".gz")));

                clearstatcache();
                $this->assertFalse($rawFile->isFile());

                exec("gunzip -d " . escapeshellarg($archiveFile), $output, $return);
                $this->assertEquals(0, $return);

                clearstatcache();
                $this->assertTrue($rawFile->isFile());
            }
        }
        return $columns;
    }
}
