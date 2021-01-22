<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Process\Process;

class SnowflakeEntrypointTest extends AbstractSnowflakeTest
{
    public const DRIVER = 'snowflake';

    public const ROOT_PATH = __DIR__ . '/db-extractor-snowflake';

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

        @unlink($dataPath . '/out/tables/in.c-main.sales.csv.gz');
        @unlink($dataPath . '/out/tables/in.c-main.sales.csv.gz.manifest');

        @unlink($dataPath . '/out/tables/in.c-main.escaping.csv.gz');
        @unlink($dataPath . '/out/tables/in.c-main.escaping.csv.gz.manifest');

        @unlink($dataPath . '/out/tables/in.c-main.tableColumns.csv.gz');
        @unlink($dataPath . '/out/tables/in.c-main.tableColumns.csv.gz.manifest');

        $this->createConfigFile($dataPath);

        $process = Process::fromShellCommandline('php /code/src/run.php');
        $process->setEnv(['KBC_DATADIR' => $dataPath]);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode(), sprintf('error output: %s', $process->getOutput()));
        $this->assertFileExists($dataPath . '/out/tables/in.c-main.sales.csv.gz');
        $this->assertFileExists($dataPath . '/out/tables/in.c-main.sales.csv.gz.manifest');
        $this->assertFileExists($dataPath . '/out/tables/in.c-main.escaping.csv.gz');
        $this->assertFileExists($dataPath . '/out/tables/in.c-main.escaping.csv.gz.manifest');
        $this->assertFileExists($dataPath . '/out/tables/in.c-main.tableColumns.csv.gz');
        $this->assertFileExists($dataPath . '/out/tables/in.c-main.tableColumns.csv.gz.manifest');
    }

    /**
     * @dataProvider getRowFilesProvider
     */
    public function testRunRowAction(string $rowFile): void
    {
        $dataPath = __DIR__ . '/data/runAction';

        @unlink($dataPath . '/out/tables/in.c-main.escaping.csv.gz');
        @unlink($dataPath . '/out/tables/in.c-main.escaping.csv.gz.manifest');

        $this->createConfigFile($dataPath, $rowFile);

        $process = Process::fromShellCommandline('php /code/src/run.php');
        $process->setEnv(['KBC_DATADIR' => $dataPath]);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode(), sprintf('error output: %s', $process->getErrorOutput()));
        $this->assertFileExists($dataPath . '/out/tables/in.c-main.escaping.csv.gz');
        $this->assertFileExists($dataPath . '/out/tables/in.c-main.escaping.csv.gz.manifest');
    }

    public function testRunInvalidConfig(): void
    {
        $config = $this->getConfig();
        unset($config['parameters']['tables'][0]['outputTable']);
        @unlink($this->dataDir . '/config.json');
        file_put_contents($this->dataDir . '/config.json', json_encode($config));

        $process = Process::fromShellCommandline('php /code/src/run.php');
        $process->setEnv(['KBC_DATADIR' => $this->dataDir]);
        $process->run();

        $this->assertEquals(1, $process->getExitCode());
        $this->assertEquals(
            trim($process->getErrorOutput()),
            'The child config "outputTable" under "root.parameters.tables.0" must be configured.'
        );
    }

    public function testConnectionAction(): void
    {
        $dataPath = __DIR__ . '/data/connectionAction';

        $this->createConfigFile($dataPath);

        $process = Process::fromShellCommandline('php /code/src/run.php');
        $process->setEnv(['KBC_DATADIR' => $dataPath]);
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

        $process = Process::fromShellCommandline('php /code/src/run.php');
        $process->setEnv(['KBC_DATADIR' => $dataPath]);
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

        $process = Process::fromShellCommandline('php /code/src/run.php');
        $process->setEnv(['KBC_DATADIR' => $this->dataDir]);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(1, $process->getExitCode());
    }

    public function testGetTablesAction(): void
    {
        $dataPath = __DIR__ . '/data/getTablesAction';

        $this->createConfigFile($dataPath);

        $process = Process::fromShellCommandline('php /code/src/run.php');
        $process->setEnv(['KBC_DATADIR' => $dataPath]);
        $process->setTimeout(300);
        $process->run();

        $this->assertJson($process->getOutput());
        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals('', $process->getErrorOutput());
    }

    public function testBadTypesRetries(): void
    {
        $this->markTestSkipped();
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

    public function testRunIncrementalFetching(): void
    {
        $config = $this->getIncrementalConfig();
        $this->createAutoIncrementAndTimestampTable($config);

        @unlink($this->dataDir . '/config.json');

        $inputStateFile = $this->dataDir . '/in/state.json';

        $fs = new Filesystem();
        if (!$fs->exists($inputStateFile)) {
            $fs->mkdir($this->dataDir . '/in');
            $fs->touch($inputStateFile);
        }
        $outputStateFile = $this->dataDir . '/out/state.json';
        // unset the state file
        @unlink($outputStateFile);
        @unlink($inputStateFile);

        file_put_contents($this->dataDir . '/config.json', json_encode($config));

        $process = Process::fromShellCommandline('php /code/src/run.php');
        $process->setEnv(['KBC_DATADIR' => $this->dataDir]);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertFileExists($outputStateFile);
        $this->assertEquals(['lastFetchedRow' => '2'], json_decode((string) file_get_contents($outputStateFile), true));

        // add a couple rows
        $this->connection->query(sprintf(
            "INSERT INTO %s.%s (\"name\") VALUES ('wiliam'), ('charles')",
            $this->connection->quoteIdentifier($config['parameters']['table']['schema']),
            $this->connection->quoteIdentifier($config['parameters']['table']['tableName'])
        ));

        // copy state to input state file
        file_put_contents($inputStateFile, file_get_contents($outputStateFile));

        // run the config again
        $process = Process::fromShellCommandline('php /code/src/run.php');
        $process->setEnv(['KBC_DATADIR' => $this->dataDir]);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals(['lastFetchedRow' => '4'], json_decode((string) file_get_contents($outputStateFile), true));
    }

    public function getRowFilesProvider(): array
    {
        return [
            ['config.row.template.json'],
            ['config.rowTable.template.json'],
        ];
    }
}
