<?php
namespace Keboola\DbExtractor;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Snowflake\Connection;
use Symfony\Component\Yaml\Yaml;

class SnowflakeTest extends AbstractSnowflakeTest
{
    private function getUserDefaultWarehouse($user)
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

    private function setUserDefaultWarehouse($user, $warehouse = null)
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

    public function testDefaultWarehouse()
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
        } catch (\Exception $e) {
            $this->assertRegExp('/No active warehouse/ui', $e->getMessage());
        }

        // run with warehouse param
        $config = $this->getConfig();
        $app = $this->createApplication($config);

        $result = $app->run();
        $this->assertEquals('success', $result['status']);
        $this->assertCount(2, $result['imported']);

        $this->setUserDefaultWarehouse($user, $warehouse);
    }

    public function testCredentials()
    {
        $config = $this->getConfig();
        $config['action'] = 'testConnection';
        unset($config['parameters']['tables']);

        $app = $this->createApplication($config);
        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertEquals('success', $result['status']);
    }

    public function testCredentialsDefaultWarehouse()
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

    public function testRunWithoutTables()
    {
        $config = $this->getConfig();

        unset($config['parameters']['tables']);

        $app = $this->createApplication($config);
        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertEquals('success', $result['status']);
    }

    public function testRun()
    {
        $config = $this->getConfig();
        $app = $this->createApplication($config);

        $csv1 = new CsvFile($this->dataDir . '/snowflake/sales.csv');
        $this->createTextTable($csv1);
        $header1 = $csv1->getHeader();

        $csv2 = new CsvFile($this->dataDir . '/snowflake/escaping.csv');
        $this->createTextTable($csv2);
        $header2 = $csv2->getHeader();

        $result = $app->run();
        $this->assertEquals('success', $result['status']);
        $this->assertCount(2, $result['imported']);

        $columns = [];
        foreach ($config['parameters']['tables'] as $table) {
            $columns[] = $this->validateExtraction($table, $table['enabled'] ? 1 : 0);
        }

        // validate columns with file header
        $this->assertEquals($header1, $columns[0]);
        $this->assertEquals($header2, $columns[1]);

        // remove header
        $csv1arr = iterator_to_array($csv1);
        array_shift($csv1arr);
        $outCsv1 = new CsvFile($this->dataDir . '/out/tables/in_c-main_sales.csv.gz/part_0_0_0.csv');
        $this->assertEquals($csv1arr, iterator_to_array($outCsv1));

        $csv2arr = iterator_to_array($csv2);
        array_shift($csv2arr);
        $outCsv2 = new CsvFile($this->dataDir . '/out/tables/in_c-main_escaping.csv.gz/part_0_0_0.csv');
        $this->assertEquals($csv2arr, iterator_to_array($outCsv2));
    }

    private function validateExtraction(array $query, $expectedFiles = 1)
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
            return;
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

    public function testRunEmptyQuery()
    {
        $csv = new CsvFile($this->dataDir . '/snowflake/escaping.csv');
        $this->createTextTable($csv);

        $outputCsvFolder = $this->dataDir . '/out/tables/in.c-main.escaping.csv';
        $outputManifestFile = $this->dataDir . '/out/tables/in.c-main.escaping.csv.manifest';
        @unlink($outputCsvFolder);
        @unlink($outputManifestFile);

        $config = $this->getConfig();
        $config['parameters']['tables'][0]['query'] = "SELECT * FROM \"escaping\" WHERE col1 = '123'";

        $app = $this->createApplication($config);
        $result = $app->run();

        $this->assertEquals('success', $result['status']);
        $this->assertFileNotExists($outputCsvFolder);
        $this->assertFileNotExists($outputManifestFile);
    }

    public function testGetTablesPlease()
    {
        $config = $this->getConfig();
        $config['action'] = 'getTables';

        $app = $this->createApplication($config);
        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertArrayHasKey('tables', $result);
        $this->assertEquals('success', $result['status']);
        $this->assertCount(2, $result['tables']);
        foreach ($result['tables'] as $table) {
            $this->assertArrayHasKey('name', $table);
            $this->assertArrayHasKey('schema', $table);
            $this->assertArrayHasKey('type', $table);
            $this->assertArrayHasKey('columns', $table);
            if ($table['name'] === "escaping") {
                continue;
            }
            $this->assertEquals($config['parameters']['db']['database'], $table['catalog']);
            $this->assertEquals($config['parameters']['db']['schema'], $table['schema']);
            $this->assertEquals('TRANSIENT', $table['type']);
            $this->assertEquals(100, $table['rowCount']);
            $this->assertEquals(11264, $table['byteCount']);
            $this->assertCount(12, $table['columns']);
            foreach ($table['columns'] as $i => $column) {
                $this->assertArrayHasKey('name', $column);
                $this->assertArrayHasKey('type', $column);
                $this->assertArrayHasKey('length', $column);
                $this->assertArrayHasKey('default', $column);
                $this->assertArrayHasKey('nullable', $column);
                // values
                $this->assertEquals("TEXT", $column['type']);
                $this->assertEquals(200, $column['length']);
                $this->assertFalse($column['nullable']);
                $this->assertNull($column['default']);
            }
        }
    }

    public function testManifestMetadata()
    {
        $config = $this->getConfig();

        $config['parameters']['tables'][0]['columns'] = ["usergender","usercity","usersentiment","zipcode", "createdat"];
        $config['parameters']['tables'][0]['table'] = 'sales';
        $config['parameters']['tables'][0]['query'] = "SELECT \"usergender\", \"usercity\", \"usersentiment\", \"zipcode\", \"createdat\" FROM \"sales\"";
        // use just 1 table
        unset($config['parameters']['tables'][1]);
        unset($config['parameters']['tables'][2]);

        $app = $this->createApplication($config);

        $result = $app->run();

        $outputManifest = Yaml::parse(
            file_get_contents($this->dataDir . '/out/tables/in_c-main_sales.csv.gz.manifest')
        );

        $this->assertArrayHasKey('destination', $outputManifest);
        $this->assertArrayHasKey('incremental', $outputManifest);
        $this->assertArrayHasKey('metadata', $outputManifest);
        foreach ($outputManifest['metadata'] as $i => $metadata) {
            $this->assertArrayHasKey('key', $metadata);
            $this->assertArrayHasKey('value', $metadata);
            switch ($metadata['key']) {
                case 'KBC.name':
                    $this->assertEquals('sales', $metadata['value']);
                    break;
                case 'KBC.catalog':
                    $this->assertEquals($config['parameters']['db']['database'], $metadata['value']);
                    break;
                case 'KBC.schema':
                    $this->assertEquals($config['parameters']['db']['schema'], $metadata['value']);
                    break;
                case 'KBC.type':
                    $this->assertEquals('TRANSIENT', $metadata['value']);
                    break;
                case 'KBC.rowCount':
                    $this->assertEquals('100', $metadata['value']);
                    break;
                case 'KBC.byteCount':
                    $this->assertEquals('11264', $metadata['value']);
                    break;
                default:
                    $this->fail('Unknown table metadata key: ' . $metadata['key']);
            }
        }
        $this->assertArrayHasKey('column_metadata', $outputManifest);
        $this->assertCount(5, $outputManifest['column_metadata']);
        foreach ($outputManifest['column_metadata']['createdat'] as $metadata) {
            $this->assertArrayHasKey('key', $metadata);
            $this->assertArrayHasKey('value', $metadata);
            switch ($metadata['key']) {
                case 'KBC.datatype.type':
                    $this->assertEquals('TEXT', $metadata['value']);
                    break;
                case 'KBC.datatype.basetype':
                    $this->assertEquals('STRING', $metadata['value']);
                    break;
                case 'KBC.datatype.nullable':
                    $this->assertFalse($metadata['value']);
                    break;
                case 'KBC.datatype.default':
                    $this->assertNull($metadata['value']);
                    break;
                case 'KBC.datatype.length':
                    $this->assertEquals('200', $metadata['value']);
                    break;
                case 'KBC.ordinalPosition':
                    $this->assertGreaterThan(1, $metadata['value']);
                    break;
                default:
                    break;
            }
        }
    }
}
