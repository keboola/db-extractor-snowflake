<?php
namespace Keboola\DbWriter\Writer;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\AbstractSnowflakeTest;
use Symfony\Component\Yaml\Yaml;

class SnowflakeEntrypointTest extends AbstractSnowflakeTest
{
    private function createConfigFile($rootPath)
    {
        $driver = 'snowflake';

        $config = Yaml::parse(file_get_contents($rootPath . '/config.template.yml'));
        $config['parameters']['db']['user'] = $this->getEnv($driver, 'DB_USER', true);
        $config['parameters']['db']['#password'] = $this->getEnv($driver, 'DB_PASSWORD', true);
        $config['parameters']['db']['schema'] = $this->getEnv($driver, 'DB_SCHEMA');
        $config['parameters']['db']['host'] = $this->getEnv($driver, 'DB_HOST');
        $config['parameters']['db']['port'] = $this->getEnv($driver, 'DB_PORT');
        $config['parameters']['db']['database'] = $this->getEnv($driver, 'DB_DATABASE');
        $config['parameters']['db']['warehouse'] = $this->getEnv($driver, 'DB_WAREHOUSE');

        file_put_contents($rootPath . '/config.yml', Yaml::dump($config));

        return new \SplFileInfo($rootPath . '/config.yml');
    }

    public function testRunAction()
    {
        $rootPath = __DIR__ . '/../../..';
        $dataPath = __DIR__ . '/../../data/runAction';

        $csv1 = new CsvFile($this->dataDir . '/snowflake/sales.csv');
        $this->createTextTable($csv1);

        $csv2 = new CsvFile($this->dataDir . '/snowflake/escaping.csv');
        $this->createTextTable($csv2);

        $this->createConfigFile($dataPath);

        $lastOutput = exec('php ' . $rootPath . '/run.php --data=' . $dataPath . ' 2>&1', $output, $returnCode);

        $this->assertEquals(0, $returnCode);
        $this->assertGreaterThan(1, count($output));
    }

    public function testConnectionAction()
    {
        $rootPath = __DIR__ . '/../../..';
        $dataPath = __DIR__ . '/../../data/connectionAction';

        $this->createConfigFile($dataPath);

        $lastOutput = exec('php ' . $rootPath . '/run.php --data=' . $dataPath . ' 2>&1', $output, $returnCode);

        $this->assertEquals(0, $returnCode);

        $this->assertCount(1, $output);
        $this->assertEquals($lastOutput, reset($output));

        $data = json_decode($lastOutput, true);
        $this->assertArrayHasKey('status', $data);
        $this->assertEquals('success', $data['status']);
    }
}
