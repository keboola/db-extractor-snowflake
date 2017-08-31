<?php
namespace Keboola\DbWriter\Writer;

use Keboola\DbExtractor\AbstractSnowflakeTest;
use Symfony\Component\Process\Process;
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

        @unlink($dataPath . "/out/tables/in.c-main_sales.csv.gz");
        @unlink($dataPath . "/out/tables/in.c-main_sales.csv.gz.manifest");

        @unlink($dataPath . "/out/tables/in.c-main_escaping.csv.gz");
        @unlink($dataPath . "/out/tables/in.c-main_escaping.csv.gz.manifest");

        $this->createConfigFile($dataPath);

        $process = new Process('php ' . $rootPath . '/run.php --data=' . $dataPath . ' 2>&1');
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertFileExists($dataPath . "/out/tables/in_c-main_sales.csv.gz");
        $this->assertFileExists($dataPath . "/out/tables/in_c-main_sales.csv.gz.manifest");
        $this->assertFileExists($dataPath . "/out/tables/in_c-main_escaping.csv.gz");
        $this->assertFileExists($dataPath . "/out/tables/in_c-main_escaping.csv.gz.manifest");
    }

    public function testConnectionAction()
    {
        $rootPath = __DIR__ . '/../../..';
        $dataPath = __DIR__ . '/../../data/connectionAction';

        $this->createConfigFile($dataPath);

        $process = new Process('php ' . $rootPath . '/run.php --data=' . $dataPath . ' 2>&1');
        $process->run();

        $output = $process->getOutput();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertJson($output);

        $data = json_decode($output, true);
        $this->assertArrayHasKey('status', $data);
        $this->assertEquals('success', $data['status']);
    }

    public function testNonexistingTable()
    {
        $config = $this->getConfig();
        $config['parameters']['tables'][0]['query'] = "SELECT * FROM non_existing_table";
        @unlink($this->dataDir . '/config.yml');
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        $process = new Process('php ' . ROOT_PATH . '/run.php --data=' . $this->dataDir);
        $process->run();

        var_dump($process->getOutput());
        var_dump($process->getErrorOutput());

        $this->assertEquals(1, $process->getExitCode());
    }

    public function testGetTablesAction()
    {
        $config = $this->getConfig();
        @unlink($this->dataDir . '/config.yml');
        $config['action'] = 'getTables';
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        $process = new Process('php ' . ROOT_PATH . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        var_dump($process->getErrorOutput());
        var_dump($process->getOutput());

        $this->assertJson($process->getOutput());
        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals("", $process->getErrorOutput());
    }

    public function testTableColumnsQuery()
    {
        $config = $this->getConfig();
        @unlink($this->dataDir . '/config.yml');
        unset($config['tables'][0]);
        unset($config['tables'][1]);

        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        $process = new Process('php ' . ROOT_PATH . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        var_dump($process->getErrorOutput());
        var_dump($process->getOutput());

        $this->assertEquals(0, $process->getExitCode());
        $this->assertFileExists($this->dataDir . "/out/tables/in_c-main_tableColumns.csv.gz");
        $this->assertFileExists($this->dataDir . "/out/tables/in_c-main_tableColumns.csv.gz.manifest");
    }
}
