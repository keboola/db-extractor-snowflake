<?php
namespace Keboola\DbExtractor;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Snowflake\Connection;
use Symfony\Component\Yaml\Yaml;

class SnowflakeTest extends AbstractSnowflakeTest
{
    public function setUp()
    {
        if (!defined('APP_NAME')) {
            define('APP_NAME', 'ex-db-snowflake');
        }

        $config = $this->getConfig();

        $this->connection = new Connection($config['parameters']['db']);
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

        $csv2 = new CsvFile($this->dataDir . '/snowflake/escaping.csv');
        $this->createTextTable($csv2);

        $result = $app->run();
        $this->assertEquals('success', $result['status']);
        $this->assertCount(2, $result['imported']);

        foreach ($config['parameters']['tables'] as $table) {
            $this->validateExtraction($table, $table['enabled'] ? 1 : 0);
        }

        $outCsv1 = new CsvFile($this->dataDir . '/out/tables/in_c-main_sales_0_0_0.csv');
        $this->assertFileEquals((string) $csv1, (string) $outCsv1);

        $outCsv2 = new CsvFile($this->dataDir . '/out/tables/in_c-main_escaping_0_0_0.csv');
        $this->assertFileEquals((string) $csv2, (string) $outCsv2);
    }

    private function validateExtraction(array $query, $expectedFiles = 1)
    {
        $dirPath = $this->dataDir . '/out/tables';
        $outputTable = $query['outputTable'];

        $files = array_map(
            function ($fileName) use ($dirPath) {
                return $dirPath . '/' . $fileName;
            },
            array_filter(
                scandir($dirPath),
                function ($fileName) use ($dirPath, $outputTable) {
                    $filePath = $dirPath . '/' . $fileName;
                    if (!is_file($filePath)) {
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

        $this->assertCount($expectedFiles, $files);

        foreach ($files as $file) {
            // manifest validation
            $params = Yaml::parse(file_get_contents($file));

            $this->assertArrayHasKey('destination', $params);
            $this->assertArrayHasKey('incremental', $params);
            $this->assertArrayHasKey('primary_key', $params);

            if ($query['primaryKey']) {
                $this->assertEquals($query['primaryKey'], $params['primary_key']);
            } else {
                $this->assertEmpty($params['primary_key']);
            }

            $this->assertEquals($query['incremental'], $params['incremental']);

            if (isset($query['outputTable'])) {
                $this->assertEquals($query['outputTable'], $params['destination']);
            }

            // archive validation
            $csvFile = new \SplFileInfo(str_replace('.manifest', '', $file));

            clearstatcache();
            $this->assertTrue($csvFile->isFile());
        }
    }
}
