<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\FunctionalTests;

use Keboola\DatadirTests\DatadirTestCase;
use Keboola\DatadirTests\Exception\DatadirTestsException;
use Keboola\DbExtractor\TraitTests\CloseSshTunnelsTrait;
use Keboola\DbExtractor\TraitTests\RemoveAllTablesTrait;
use Keboola\SnowflakeDbAdapter\Connection;
use RuntimeException;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Process\Process;
use \Throwable;

class DatadirTest extends DatadirTestCase
{
    use RemoveAllTablesTrait;
    use CloseSshTunnelsTrait;

    protected Connection $connection;

    protected string $testProjectDir;

    protected string $testTempDir;

    public function getConnection(): Connection
    {
        return $this->connection;
    }

    public function assertDirectoryContentsSame(string $expected, string $actual): void
    {
        $this->replaceEnvInManifest($actual);
        $this->prettifyAllManifests($actual);
        $this->ungzipFiles($actual);
        parent::assertDirectoryContentsSame($expected, $actual);
    }

    protected function setUp(): void
    {
        parent::setUp();
        putenv('KBC_COMPONENT_RUN_MODE=run');

        // Test dir, eg. "/code/tests/functional/full-load-ok"
        $this->testProjectDir = $this->getTestFileDir() . '/' . $this->dataName();
        $this->testTempDir = $this->temp->getTmpFolder();

        $this->connection = TestConnection::createConnection();
        $this->removeAllTables();
        $this->closeSshTunnels();

        // Load setUp.php file - used to init database state
        $setUpPhpFile = $this->testProjectDir . '/setUp.php';
        if (file_exists($setUpPhpFile)) {
            // Get callback from file and check it
            $initCallback = require $setUpPhpFile;
            if (!is_callable($initCallback)) {
                throw new RuntimeException(sprintf('File "%s" must return callback!', $setUpPhpFile));
            }

            // Invoke callback
            $initCallback($this);
        }
    }

    protected function prettifyAllManifests(string $actual): void
    {
        foreach ($this->findManifests($actual . '/tables') as $file) {
            $this->prettifyJsonFile((string) $file->getRealPath());
        }
    }

    protected function prettifyJsonFile(string $path): void
    {
        $json = (string) file_get_contents($path);
        try {
            file_put_contents($path, (string) json_encode(json_decode($json), JSON_PRETTY_PRINT));
        } catch (Throwable $e) {
            // If a problem occurs, preserve the original contents
            file_put_contents($path, $json);
        }
    }

    protected function replaceEnvInManifest(string $actual): void
    {
        foreach ($this->findManifests($actual . '/tables') as $file) {
            $filePath = (string) $file->getRealPath();
            $json = (string) file_get_contents($filePath);
            try {
                file_put_contents(
                    $filePath,
                    str_ireplace(
                        [
                            sprintf('"%s"', getenv('SNOWFLAKE_DB_SCHEMA')),
                            sprintf('"%s"', getenv('SNOWFLAKE_DB_DATABASE')),
                        ],
                        '"replaceEnv"',
                        $json
                    )
                );
            } catch (Throwable $e) {
                // If a problem occurs, preserve the original contents
                file_put_contents($filePath, $json);
            }
        }
    }

    protected function findManifests(string $dir): Finder
    {
        $finder = new Finder();
        return $finder->files()->in($dir)->name(['~.*\.manifest~']);
    }

    protected function ungzipFiles(string $actualDir): void
    {
        $fs = new Filesystem();
        if (!$fs->exists($actualDir . '/tables')) {
            return;
        }
        $gzipFiles = $this->findGzipFiles($actualDir . '/tables');
        foreach ($gzipFiles as $gzipFile) {
            $process = Process::fromShellCommandline('gzip -d ' . $gzipFile->getRealPath());
            $process->run();
        }
    }

    public static function tearDownAfterClass(): void
    {
        $databaseManager = new DatabaseManager(TestConnection::createConnection());

        $databaseManager->createSalesTable();
        $databaseManager->generateSalesRows();
        $databaseManager->addSalesConstraint('sales', ['createdat']);

        $databaseManager->createEscapingTable();
        $databaseManager->generateEscapingRows();
        parent::tearDownAfterClass();
    }

    private function findGzipFiles(string $dir): Finder
    {
        $finder = new Finder();
        return $finder->files()->in($dir)->depth(1)->name(['~.*\.csv.gz$~']);
    }

    protected function runScript(string $datadirPath): Process
    {
        $fs = new Filesystem();

        $script = $this->getScript();
        if (!$fs->exists($script)) {
            throw new DatadirTestsException(sprintf(
                'Cannot open script file "%s"',
                $script
            ));
        }

        $runCommand = [
            'php',
            $script,
        ];
        $runProcess = new Process($runCommand);
        $runProcess->setEnv([
            'KBC_DATADIR' => $datadirPath,
            'KBC_COMPONENT_RUN_MODE' => (string) getenv('KBC_COMPONENT_RUN_MODE'),
        ]);
        $runProcess->setTimeout(0.0);
        $runProcess->run();
        return $runProcess;
    }
}
