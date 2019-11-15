<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Exception\UserException;
use Symfony\Component\Process\Process;

class SnowflakeEntrypointTest extends AbstractSnowflakeTest
{
    public const DRIVER = 'snowflake';

    public const ROOT_PATH = __DIR__ . '/..';

    private function createConfigFile(string $rootPath, string $file = 'config.template.json'): void
    {
        $config = json_decode((string) file_get_contents($rootPath . '/' . $file), true);
        $config['parameters']['db']['user'] = $this->getEnv(self::DRIVER, 'DB_USER', true);
        $config['parameters']['db']['#password'] = $this->getEnv(self::DRIVER, 'DB_PASSWORD', true);
        $config['parameters']['db']['schema'] = $this->getEnv(self::DRIVER, 'DB_SCHEMA');
        $config['parameters']['db']['host'] = $this->getEnv(self::DRIVER, 'DB_HOST');
        $config['parameters']['db']['port'] = $this->getEnv(self::DRIVER, 'DB_PORT');
        $config['parameters']['db']['database'] = $this->getEnv(self::DRIVER, 'DB_DATABASE');
        $config['parameters']['db']['warehouse'] = $this->getEnv(self::DRIVER, 'DB_WAREHOUSE');

        if (isset($config['parameters']['tables'][2]['table'])) {
            $config['parameters']['tables'][2]['table']['schema'] = $this->getEnv(self::DRIVER, 'DB_SCHEMA');
        } elseif (isset($config['parameters']['table'])) {
            $config['parameters']['table']['schema'] = $this->getEnv(self::DRIVER, 'DB_SCHEMA');
        }

        $this->putConfigFile($rootPath, $config);
    }

    private function putConfigFile(string $rootPath, array $config): void
    {
        @unlink($rootPath . '/config.json');
        file_put_contents($rootPath . '/config.json', json_encode($config));
    }

    public function testRunAction(): void
    {
        $dataPath = __DIR__ . '/data/runAction';

        @unlink($dataPath . '/out/tables/in.c-main_sales.csv.gz');
        @unlink($dataPath . '/out/tables/in.c-main_sales.csv.gz.manifest');

        @unlink($dataPath . '/out/tables/in.c-main_escaping.csv.gz');
        @unlink($dataPath . '/out/tables/in.c-main_escaping.csv.gz.manifest');

        @unlink($dataPath . '/out/tables/in.c-main_tableColumns.csv.gz');
        @unlink($dataPath . '/out/tables/in.c-main_tableColumns.csv.gz.manifest');

        $this->createConfigFile($dataPath);

        $process = Process::fromShellCommandline('php ' . self::ROOT_PATH . '/run.php --data=' . $dataPath . ' 2>&1');
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
     * @dataProvider getRowFilesProvider
     */
    public function testRunRowAction(string $rowFile): void
    {
        $dataPath = __DIR__ . '/data/runAction';

        @unlink($dataPath . '/out/tables/in.c-main_escaping.csv.gz');
        @unlink($dataPath . '/out/tables/in.c-main_escaping.csv.gz.manifest');

        $this->createConfigFile($dataPath, $rowFile);

        $process = Process::fromShellCommandline('php ' . self::ROOT_PATH . '/run.php --data=' . $dataPath . ' 2>&1');
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode(), sprintf('error output: %s', $process->getErrorOutput()));
        $this->assertFileExists($dataPath . '/out/tables/in_c-main_escaping.csv.gz');
        $this->assertFileExists($dataPath . '/out/tables/in_c-main_escaping.csv.gz.manifest');
    }

    public function testRunInvalidConfig(): void
    {
        $config = $this->getConfig();
        unset($config['parameters']['tables'][0]['id']);
        @unlink($this->dataDir . '/config.json');
        file_put_contents($this->dataDir . '/config.json', json_encode($config));

        $process = Process::fromShellCommandline(
            'php ' . self::ROOT_PATH . '/run.php --data=' . $this->dataDir . ' 2>&1'
        );
        $process->run();

        $this->assertEquals(1, $process->getExitCode());
        $this->assertEquals(
            $process->getOutput(),
            "The child node \"id\" at path \"root.parameters.tables.0\" must be configured.\n"
        );
    }

    public function testConnectionAction(): void
    {
        $dataPath = __DIR__ . '/data/connectionAction';

        $this->createConfigFile($dataPath);

        $process = Process::fromShellCommandline('php ' . self::ROOT_PATH . '/run.php --data=' . $dataPath . ' 2>&1');
        $process->run();

        $output = $process->getOutput();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertJson($output);

        $data = json_decode($output, true);
        $this->assertArrayHasKey('status', $data);
        $this->assertEquals('success', $data['status']);
    }

    public function testRowConnectionAction(): void
    {
        $dataPath = __DIR__ . '/data/connectionAction';

        $this->createConfigFile($dataPath, 'config.row.template.json');

        $process = Process::fromShellCommandline('php ' . self::ROOT_PATH . '/run.php --data=' . $dataPath . ' 2>&1');
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
        $this->putConfigFile($this->dataDir, $config);

        $process = Process::fromShellCommandline('php ' . self::ROOT_PATH . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(1, $process->getExitCode());
    }

    public function testGetTablesAction(): void
    {
        $dataPath = __DIR__ . '/data/getTablesAction';

        $this->createConfigFile($dataPath);

        $process = Process::fromShellCommandline('php ' . self::ROOT_PATH . '/run.php --data=' . $dataPath);
        $process->setTimeout(300);
        $process->run();

        $this->assertJson($process->getOutput());
        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals('', $process->getErrorOutput());
    }

    public function testBadTypesRetries(): void
    {
        $config = $this->getConfig();
        $this->createTextTable(new CsvFile($this->dataDir . '/snowflake/badTypes.csv'), 'types');
        $table = $config['parameters']['tables'][0];
        $table['name'] = 'badTypes';
        $table['query'] = 'SELECT CAST("decimal" AS DECIMAL(15,5)), "character", "integer", "date" FROM "types"';
        $table['outputTable'] = 'in.c-main.badTypes';
        unset($config['parameters']['tables']);
        $config['parameters']['tables'] = [$table];

        $this->putConfigFile($this->dataDir, $config);

        $process = Process::fromShellCommandline('php ' . self::ROOT_PATH . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        // make sure we tried 4 additional times
        $this->assertStringContainsString('[4x]', $process->getOutput());
        $this->assertStringContainsString('failed with message:', $process->getErrorOutput());
        $this->assertEquals(1, $process->getExitCode());
    }

    public function getRowFilesProvider(): array
    {
        return [
            ['config.row.template.json'],
            ['config.rowTable.template.json'],
        ];
    }
}
