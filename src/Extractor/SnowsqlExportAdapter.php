<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Exception;
use Keboola\Csv\CsvOptions;
use Keboola\DbExtractor\Adapter\ExportAdapter;
use Keboola\DbExtractor\Adapter\ODBC\OdbcConnection;
use Keboola\DbExtractor\Adapter\ValueObject\ExportResult;
use Keboola\DbExtractor\Configuration\ValueObject\SnowflakeDatabaseConfig;
use Keboola\DbExtractor\Utils\AccountUrlParser;
use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Keboola\DbExtractorConfig\Exception\InvalidArgumentException;
use Keboola\Temp\Temp;
use Psr\Log\LoggerInterface;
use SplFileInfo;
use Symfony\Component\Process\Process;

class SnowsqlExportAdapter implements ExportAdapter
{
    protected SnowflakeQueryFactory $queryFactory;

    protected SnowflakeMetadataProvider $metadataProvider;

    private OdbcConnection $connection;

    protected LoggerInterface $logger;

    private SnowflakeDatabaseConfig $databaseConfig;

    private Temp $tempDir;

    private SplFileInfo $snowSqlConfig;

    public function __construct(
        LoggerInterface $logger,
        OdbcConnection $connection,
        SnowflakeQueryFactory $QueryFactory,
        SnowflakeMetadataProvider $metadataProvider,
        DatabaseConfig $databaseConfig,
    ) {
        if (!($databaseConfig instanceof SnowflakeDatabaseConfig)) {
            throw new InvalidArgumentException('DatabaseConfig must be instance of SnowflakeDatabaseConfig');
        }
        $this->logger = $logger;
        $this->connection = $connection;
        $this->queryFactory = $QueryFactory;
        $this->metadataProvider = $metadataProvider;
        $this->databaseConfig = $databaseConfig;
        $this->tempDir = new Temp('ex-snowflake-adapter');
        $this->snowSqlConfig = $this->createSnowSqlConfig($databaseConfig);
    }

    public function getName(): string
    {
        return 'Snowsql';
    }

    public function export(ExportConfig $exportConfig, string $csvFilePath): ExportResult
    {
        // Create query
        $query = $exportConfig->hasQuery() ?
            $exportConfig->getQuery() : $this->queryFactory->create($exportConfig, $this->connection);

        // Copy into internal staging
        $this->cleanupTableStage($exportConfig->getOutputTable());
        $copyCommand = $this->generateCopyCommand($exportConfig->getOutputTable(), $query);
        $res = $this->connection->query($copyCommand)->fetchAll();
        $rowCount = (int) ($res[0]['rows_unloaded'] ?? 0);

        $this->logger->info(sprintf('%d rows copied to internal staging.', $rowCount));

        // Download CSV using snowsql
        $process = $this->runDownloadCommand($exportConfig, $csvFilePath);

        // Parse process output
        $csvFiles = $this->parseFiles($process->getOutput(), $csvFilePath);
        $bytesDownloaded = 0;
        foreach ($csvFiles as $csvFile) {
            $bytesDownloaded += $csvFile->getSize();
        }

        // Log info
        $this->logger->info(sprintf(
            '%d files (%s) downloaded.',
            count($csvFiles),
            $this->dataSizeFormatted((int) $bytesDownloaded),
        ));

        // Query metadata
        $columns = $this->metadataProvider->getColumnsInfo($query);
        $queryMetadata = new SnowflakeQueryMetadata($columns);

        // Clean-up
        $this->cleanupTableStage($exportConfig->getOutputTable());

        return new ExportResult($csvFilePath, $rowCount, $queryMetadata, false, null);
    }

    private function runDownloadCommand(ExportConfig $exportConfig, string $csvFilePath): Process
    {
        // Generate command
        $command = $this->generateDownloadSql($exportConfig, $csvFilePath);
        $this->logger->info('Downloading data from Snowflake.');
        $this->logger->debug(trim($command));

        // Run
        @mkdir($csvFilePath, 0755, true);
        $process = Process::fromShellCommandline($command);
        $process->setTimeout(null);
        $process->run();

        // Check result
        if (!$process->isSuccessful()) {
            // SnowSQL throws an error when there are no results,
            // but the expected behavior is to return process with empty output
            if (str_contains(
                $process->getErrorOutput(),
                'While getting file(s) there was an error: the file does not exist',
            )) {
                return $process;
            }
            $this->logger->error(sprintf('Snowsql error, process output %s', $process->getOutput()));
            $this->logger->error(sprintf('Snowsql error: %s', $process->getErrorOutput()));
            throw new Exception(sprintf(
                'File download error occurred processing [%s]',
                $exportConfig->hasTable() ? $exportConfig->getTable()->getName() : $exportConfig->getOutputTable(),
            ));
        }

        return $process;
    }

    private function cleanupTableStage(string $tmpTableName): void
    {
        $sql = sprintf('REMOVE @~/%s/;', $tmpTableName);
        $this->connection->query($sql);
    }

    private function dataSizeFormatted(int $bytes): string
    {
        if (!$bytes) {
            return '0 B';
        }

        $base = log($bytes) / log(1024);
        $suffixes = [' B', ' KB', ' MB', ' GB', ' TB'];
        return round(pow(1024, $base - floor($base)), 2) . $suffixes[(int) floor($base)];
    }

    private function parseFiles(string $output, string $path): array
    {
        $files = [];
        $lines = explode("\n", $output);

        $lines = array_map(
            function ($item): array {
                $item = trim($item, '|');
                return array_map('trim', explode('|', $item));
            },
            array_filter(
                $lines,
                function ($item): bool {
                    $item = trim($item);
                    return preg_match('/^\|.+\|$/ui', $item) && preg_match('/([.a-z0-9_\-]+\.gz)/ui', $item);
                },
            ),
        );

        foreach ($lines as $line) {
            if (!preg_match('/^(downloaded|uploaded)$/ui', $line[2])) {
                throw new Exception(sprintf(
                    'Cannot download file: %s Status: %s Size: %s Message: %s',
                    $line[0],
                    $line[2],
                    $line[1],
                    $line[3],
                ));
            }

            $file = new     SplFileInfo($path . '/' . $line[0]);
            if ($file->isFile()) {
                $files[] = $file;
            } else {
                throw new Exception('Missing file: ' . $line[0]);
            }
        }

        return $files;
    }

    private function generateCopyCommand(string $stageTmpPath, string $query): string
    {
        $csvOptions = [];

        $csvOptions[] = sprintf(
            'FIELD_DELIMITER = %s',
            $this->connection->quote(CsvOptions::DEFAULT_DELIMITER),
        );

        $csvOptions[] = sprintf(
            'FIELD_OPTIONALLY_ENCLOSED_BY = %s',
            $this->connection->quote(CsvOptions::DEFAULT_ENCLOSURE),
        );

        $csvOptions[] = sprintf(
            'ESCAPE_UNENCLOSED_FIELD = %s',
            $this->connection->quote('\\\\'),
        );

        $csvOptions[] = sprintf(
            'COMPRESSION = %s',
            $this->connection->quote('GZIP'),
        );

        $csvOptions[] = sprintf('NULL_IF=()');

        return sprintf(
            '
            COPY INTO @~/%s/part
            FROM (%s)
            FILE_FORMAT = (TYPE=CSV %s)
            HEADER = false
            MAX_FILE_SIZE=50000000
            OVERWRITE = TRUE
            ;
            ',
            $stageTmpPath,
            rtrim(trim($query), ';'),
            implode(' ', $csvOptions),
        );
    }

    private function generateDownloadSql(ExportConfig $exportConfig, string $outputDataDir): string
    {
        $sql = [];
        if ($this->databaseConfig->hasWarehouse()) {
            $sql[] = sprintf(
                'USE WAREHOUSE %s;',
                $this->connection->quoteIdentifier($this->databaseConfig->getWarehouse()),
            );
        }

        $sql[] = sprintf(
            'USE DATABASE %s;',
            $this->connection->quoteIdentifier($this->databaseConfig->getDatabase()),
        );

        if ($this->databaseConfig->hasSchema()) {
            $sql[] = sprintf(
                'USE SCHEMA %s.%s;',
                $this->connection->quoteIdentifier($this->databaseConfig->getDatabase()),
                $this->connection->quoteIdentifier($this->databaseConfig->getSchema()),
            );
        }

        $sql[] = sprintf(
            'GET @~/%s/ file://%s;',
            $exportConfig->getOutputTable(),
            $outputDataDir,
        );

        $snowSql = $this->tempDir->createTmpFile('snowsql.sql');
        file_put_contents($snowSql->getPathname(), implode("\n", $sql));

        $this->logger->debug(trim(implode("\n", $sql)));

        // execute external
        return sprintf(
            'snowsql --noup --config %s -c downloader -f %s',
            $this->snowSqlConfig,
            $snowSql,
        );
    }


    private function createSnowSqlConfig(SnowflakeDatabaseConfig $databaseConfig): SplFileInfo
    {
        $cliConfig[] = '';
        $cliConfig[] = '[options]';
        $cliConfig[] = 'exit_on_error = true';
        $cliConfig[] = '';
        $cliConfig[] = '[connections.downloader]';
        $cliConfig[] = sprintf('accountname = "%s"', AccountUrlParser::parse($databaseConfig->getHost()));
        $cliConfig[] = sprintf('username = "%s"', $databaseConfig->getUsername());

        if (!empty($databaseConfig->getPassword())) {
            $cliConfig[] = sprintf('password = "%s"', $databaseConfig->getPassword());
        }

        if ($databaseConfig->hasPrivateKey()) {
            $cliConfig[] = sprintf('private_key_path = "%s"', $databaseConfig->getPrivateKeyPath());
        }

        $cliConfig[] = sprintf('dbname = "%s"', $databaseConfig->getDatabase());

        if ($databaseConfig->hasWarehouse()) {
            $cliConfig[] = sprintf('warehousename = "%s"', $databaseConfig->getWarehouse());
        }

        if ($databaseConfig->hasSchema()) {
            $cliConfig[] = sprintf('schemaname = "%s"', $databaseConfig->getSchema());
        }

        $file = $this->tempDir->createFile('snowsql.config');
        file_put_contents($file->getPathname(), implode("\n", $cliConfig));

        return $file;
    }
}
