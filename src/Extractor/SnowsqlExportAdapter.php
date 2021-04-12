<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Exception;
use Keboola\Csv\CsvOptions;
use Keboola\DbExtractor\Adapter\ExportAdapter;
use Keboola\DbExtractor\Adapter\ODBC\OdbcConnection;
use Keboola\DbExtractor\Adapter\ValueObject\ExportResult;
use Keboola\DbExtractor\Configuration\ValueObject\SnowflakeDatabaseConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Keboola\DbExtractorConfig\Exception\InvalidArgumentException;
use Keboola\Temp\Temp;
use Psr\Log\LoggerInterface;
use SplFileInfo;
use Symfony\Component\Process\Process;

class SnowsqlExportAdapter implements ExportAdapter
{
    protected SnowflakeQueryFactory $simpleQueryFactory;

    private OdbcConnection $connection;

    protected LoggerInterface $logger;

    private SplFileInfo $snowSqlConfig;

    private Temp $tempDir;

    private SnowflakeDatabaseConfig $databaseConfig;

    private SnowflakeMetadataProvider $metadataProvider;

    public const SEMI_STRUCTURED_TYPES = ['VARIANT' , 'OBJECT', 'ARRAY'];

    public function __construct(
        LoggerInterface $logger,
        OdbcConnection $connection,
        SnowflakeQueryFactory $QueryFactory,
        DatabaseConfig $databaseConfig,
        SplFileInfo $snowSqlConfig,
        Temp $tempDir,
        SnowflakeMetadataProvider $metadataProvider
    ) {
        if (!($databaseConfig instanceof SnowflakeDatabaseConfig)) {
            throw new InvalidArgumentException('DatabaseConfig must be instance of SnowflakeDatabaseConfig');
        }
        $this->logger = $logger;
        $this->connection = $connection;
        $this->simpleQueryFactory = $QueryFactory;
        $this->databaseConfig = $databaseConfig;
        $this->snowSqlConfig = $snowSqlConfig;
        $this->tempDir = $tempDir;
        $this->metadataProvider = $metadataProvider;
    }

    public function getName(): string
    {
        return 'Snowsql';
    }

    public function export(ExportConfig $exportConfig, string $csvFilePath): ExportResult
    {
        if (!$exportConfig->hasQuery()) {
            $query = $this->createSimpleQuery($exportConfig);
            $columnInfo = $this->metadataProvider->getColumnInfo($query);
            $objectColumns = array_filter($columnInfo, function ($column): bool {
                return in_array($column['type'], self::SEMI_STRUCTURED_TYPES);
            });
            if (!empty($objectColumns)) {
                $query = $this->createSimpleQueryWithCasting($exportConfig, $columnInfo);
            }
        } else {
            $query = $exportConfig->getQuery();
        }

        $this->cleanupTableStage($exportConfig->getOutputTable());

        // copy into internal staging
        $copyCommand = $this->generateCopyCommand($exportConfig->getOutputTable(), $query);

        $res = $this->connection->query($copyCommand)->fetchAll();
        $rowCount = (int) ($res[0]['rows_unloaded'] ?? 0);

        if ($rowCount === 0) {
            // query resulted in no rows, nothing left to do
            return new ExportResult($csvFilePath, 0, null);
        }

        $this->logger->info('Downloading data from Snowflake');

        @mkdir($csvFilePath, 0755, true);

        $command = $this->generateDownloadSql($exportConfig, $csvFilePath);

        $this->logger->debug(trim($command));

        $process = Process::fromShellCommandline($command);
        $process->setTimeout(null);
        $process->run();

        if (!$process->isSuccessful()) {
            $this->logger->error(sprintf('Snowsql error, process output %s', $process->getOutput()));
            $this->logger->error(sprintf('Snowsql error: %s', $process->getErrorOutput()));
            throw new Exception(sprintf(
                'File download error occurred processing [%s]',
                $exportConfig->hasTable() ? $exportConfig->getTable()->getName() : $exportConfig->getOutputTable()
            ));
        }

        $csvFiles = $this->parseFiles($process->getOutput(), $csvFilePath);
        $bytesDownloaded = 0;
        foreach ($csvFiles as $csvFile) {
            $bytesDownloaded += $csvFile->getSize();
        }

        $this->logger->info(sprintf(
            '%d files (%s) downloaded',
            count($csvFiles),
            $this->dataSizeFormatted((int) $bytesDownloaded)
        ));

        $this->cleanupTableStage($exportConfig->getOutputTable());

        if ($rowCount > 0) {
            return new ExportResult($csvFilePath, $rowCount, null);
        }

        @unlink($csvFilePath); // no rows, no file
        $this->logger->warning(sprintf(
            'Query returned empty result. Nothing was imported to [%s]',
            $exportConfig->getOutputTable()
        ));

        return new ExportResult($csvFilePath, 0, null);
    }

    protected function createSimpleQuery(ExportConfig $exportConfig): string
    {
        return $this->simpleQueryFactory->create($exportConfig, $this->connection);
    }

    protected function createSimpleQueryWithCasting(ExportConfig $exportConfig, array $columnInfo): string
    {
        return $this->simpleQueryFactory->createWithCasting($exportConfig, $this->connection, $columnInfo);
    }

    private function cleanupTableStage(string $tmpTableName): void
    {
        $sql = sprintf('REMOVE @~/%s;', $tmpTableName);
        $this->connection->query($sql);
    }

    private function dataSizeFormatted(int $bytes): string
    {
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
                }
            )
        );

        foreach ($lines as $line) {
            if (!preg_match('/^downloaded$/ui', $line[2])) {
                throw new Exception(sprintf(
                    'Cannot download file: %s Status: %s Size: %s Message: %s',
                    $line[0],
                    $line[2],
                    $line[1],
                    $line[3]
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
            $this->connection->quote(CsvOptions::DEFAULT_DELIMITER)
        );

        $csvOptions[] = sprintf(
            'FIELD_OPTIONALLY_ENCLOSED_BY = %s',
            $this->connection->quote(CsvOptions::DEFAULT_ENCLOSURE)
        );

        $csvOptions[] = sprintf(
            'ESCAPE_UNENCLOSED_FIELD = %s',
            $this->connection->quote('\\\\')
        );

        $csvOptions[] = sprintf(
            'COMPRESSION = %s',
            $this->connection->quote('GZIP')
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
            implode(' ', $csvOptions)
        );
    }

    private function generateDownloadSql(ExportConfig $exportConfig, string $outputDataDir): string
    {
        $sql = [];
        if ($this->databaseConfig->hasWarehouse()) {
            $sql[] = sprintf(
                'USE WAREHOUSE %s;',
                $this->connection->quoteIdentifier($this->databaseConfig->getWarehouse())
            );
        }

        $sql[] = sprintf(
            'USE DATABASE %s;',
            $this->connection->quoteIdentifier($this->databaseConfig->getDatabase())
        );

        if ($this->databaseConfig->hasSchema()) {
            $sql[] = sprintf(
                'USE SCHEMA %s.%s;',
                $this->connection->quoteIdentifier($this->databaseConfig->getDatabase()),
                $this->connection->quoteIdentifier($this->databaseConfig->getSchema())
            );
        }

        $sql[] = sprintf(
            'GET @~/%s file://%s;',
            $exportConfig->getOutputTable(),
            $outputDataDir
        );

        $snowSql = $this->tempDir->createTmpFile('snowsql.sql');
        file_put_contents($snowSql->getPathname(), implode("\n", $sql));

        $this->logger->debug(trim(implode("\n", $sql)));

        // execute external
        return sprintf(
            'snowsql --noup --config %s -c downloader -f %s',
            $this->snowSqlConfig,
            $snowSql
        );
    }
}
