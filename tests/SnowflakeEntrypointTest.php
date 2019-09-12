<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Exception\UserException;
use Symfony\Component\Process\Process;
use Symfony\Component\Yaml\Yaml;

class SnowflakeEntrypointTest extends AbstractSnowflakeTest
{
    public const DRIVER = 'snowflake';

    public const ROOT_PATH = __DIR__ . '/..';

    private function createConfigFile(string $rootPath, string $configType = 'yaml'): void
    {
        $config = Yaml::parse((string) file_get_contents($rootPath . '/config.template.yml'));
        $config['parameters']['db']['user'] = $this->getEnv(self::DRIVER, 'DB_USER', true);
        $config['parameters']['db']['#password'] = $this->getEnv(self::DRIVER, 'DB_PASSWORD', true);
        $config['parameters']['db']['schema'] = $this->getEnv(self::DRIVER, 'DB_SCHEMA');
        $config['parameters']['db']['host'] = $this->getEnv(self::DRIVER, 'DB_HOST');
        $config['parameters']['db']['port'] = $this->getEnv(self::DRIVER, 'DB_PORT');
        $config['parameters']['db']['database'] = $this->getEnv(self::DRIVER, 'DB_DATABASE');
        $config['parameters']['db']['warehouse'] = $this->getEnv(self::DRIVER, 'DB_WAREHOUSE');

        if (isset($config['parameters']['tables'][2]['table'])) {
            $config['parameters']['tables'][2]['table']['schema'] = $this->getEnv(self::DRIVER, 'DB_SCHEMA');
        }

        // unlink any old configs written here
        @unlink($rootPath . '/config.yml');
        @unlink($rootPath . '/config.json');

        if ($configType === 'yaml') {
            file_put_contents($rootPath . '/config.yml', Yaml::dump($config));
        } else if ($configType === 'json') {
            file_put_contents($rootPath . '/config.json', json_encode($config));
        } else {
            throw new UserException(sprintf('Unsupported configType [%s]', $configType));
        }
    }

    /**
     * @dataProvider configTypesProvider
     * @param string $configType
     */
    public function testRunAction(string $configType): void
    {
        $dataPath = __DIR__ . '/data/runAction';

        @unlink($dataPath . '/out/tables/in.c-main_sales.csv.gz');
        @unlink($dataPath . '/out/tables/in.c-main_sales.csv.gz.manifest');

        @unlink($dataPath . '/out/tables/in.c-main_escaping.csv.gz');
        @unlink($dataPath . '/out/tables/in.c-main_escaping.csv.gz.manifest');

        @unlink($dataPath . '/out/tables/in.c-main_tableColumns.csv.gz');
        @unlink($dataPath . '/out/tables/in.c-main_tableColumns.csv.gz.manifest');

        $this->createConfigFile($dataPath, $configType);

        $process = new Process('php ' . self::ROOT_PATH . '/run.php --data=' . $dataPath . ' 2>&1');
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode(), sprintf('error output: %s', $process->getErrorOutput()));
        $this->assertFileExists($dataPath . '/out/tables/in_c-main_sales.csv.gz');
        $this->assertFileExists($dataPath . '/out/tables/in_c-main_sales.csv.gz.manifest');
        $this->assertFileExists($dataPath . '/out/tables/in_c-main_escaping.csv.gz');
        $this->assertFileExists($dataPath . '/out/tables/in_c-main_escaping.csv.gz.manifest');
        $this->assertFileExists($dataPath . '/out/tables/in_c-main_tableColumns.csv.gz');
        $this->assertFileExists($dataPath . '/out/tables/in_c-main_tableColumns.csv.gz.manifest');
    }

    /**
     * @dataProvider configTypesProvider
     * @param string $configType
     */
    public function testConnectionAction(string $configType): void
    {
        $dataPath = __DIR__ . '/data/connectionAction';

        $this->createConfigFile($dataPath, $configType);

        $process = new Process('php ' . self::ROOT_PATH . '/run.php --data=' . $dataPath . ' 2>&1');
        $process->run();

        $output = $process->getOutput();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertJson($output);

        $data = json_decode($output, true);
        $this->assertArrayHasKey('status', $data);
        $this->assertEquals('success', $data['status']);
    }

    public function testNonexistingTable(): void
    {
        $config = $this->getConfig();
        $config['parameters']['tables'][0]['query'] = 'SELECT * FROM non_existing_table';
        @unlink($this->dataDir . '/config.yml');
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        $process = new Process('php ' . self::ROOT_PATH . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(1, $process->getExitCode());
    }

    /**
     * @dataProvider configTypesProvider
     * @param string $configType
     */
    public function testGetTablesAction(string $configType): void
    {
        $dataPath = __DIR__ . '/data/getTablesAction';

        $this->createConfigFile($dataPath, $configType);

        $process = new Process('php ' . self::ROOT_PATH . '/run.php --data=' . $dataPath);
        $process->setTimeout(300);
        $process->run();

        $this->assertJson($process->getOutput());
        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals('', $process->getErrorOutput());
    }

    public function testBadTypesRetries(): void
    {
        $config = $this->getConfig();
        $this->createTextTable(new \Keboola\Csv\CsvFile($this->dataDir . '/snowflake/badTypes.csv'), 'types');
        $table = $config['parameters']['tables'][0];
        $table['name'] = 'badTypes';
        $table['query'] = 'SELECT CAST("decimal" AS DECIMAL(15,5)), "character", "integer", "date" FROM "types"';
        $table['outputTable'] = 'in.c-main.badTypes';
        unset($config['parameters']['tables']);
        $config['parameters']['tables'] = [$table];
        @unlink($this->dataDir . '/config.yml');
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        $process = new Process('php ' . self::ROOT_PATH . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        // make sure we tried 4 additional times
        $this->assertStringContainsString('[4x]', $process->getOutput());
        $this->assertStringContainsString('failed with message:', $process->getErrorOutput());
        $this->assertEquals(1, $process->getExitCode());
    }
}
