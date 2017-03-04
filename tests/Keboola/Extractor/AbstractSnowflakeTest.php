<?php
namespace Keboola\DbExtractor;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Snowflake\Connection;
use Keboola\DbExtractor\Test\ExtractorTest;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\GetFileOptions;

abstract class AbstractSnowflakeTest extends ExtractorTest
{
    /**
     * @var Connection
     */
    protected $connection;

    /**
     * @param string $driver
     * @return mixed
     */
    public function getConfig($driver = 'snowflake')
    {
        $config = parent::getConfig($driver);

        $config['parameters']['db']['schema'] = $this->getEnv($driver, 'DB_SCHEMA');
        $config['parameters']['db']['warehouse'] = $this->getEnv($driver, 'DB_WAREHOUSE');

        if (!empty($config['parameters']['db']['#password'])) {
            $config['parameters']['db']['password'] = $config['parameters']['db']['#password'];
        }
        $config['parameters']['extractor_class'] = 'Snowflake';

        return $config;
    }

    /**
     * @param array $config
     * @return SnowflakeApplication
     */
    public function createApplication(array $config)
    {
        $app = new SnowflakeApplication($config, $this->dataDir);

        return $app;
    }

    /**
     * @param CsvFile $file
     * @return string
     */
    protected function generateTableName(CsvFile $file)
    {
        $tableName = sprintf(
            '%s',
            $file->getBasename('.' . $file->getExtension())
        );

        return $tableName;
    }

    private function quote($value)
    {
        return "'" . addslashes($value) . "'";
    }

    private function generateCreateCommand(CsvFile $csv, $fileInfo)
    {
        $csvOptions = [];
        $csvOptions[] = sprintf('FIELD_DELIMITER = %s', $this->quote($csv->getDelimiter()));
        $csvOptions[] = sprintf("FIELD_OPTIONALLY_ENCLOSED_BY = %s", $this->quote($csv->getEnclosure()));
        $csvOptions[] = sprintf("ESCAPE_UNENCLOSED_FIELD = %s", $this->quote('\\'));

        if (!$fileInfo['isSliced']) {
            $csvOptions[] = "SKIP_HEADER = 1";
        }

        return sprintf(
            "
             COPY INTO %s
             FROM 's3://%s/%s'
             FILE_FORMAT = (TYPE=CSV %s)
             CREDENTIALS = (AWS_KEY_ID = %s AWS_SECRET_KEY = %s  AWS_TOKEN = %s)
            ",
            $this->connection->quoteIdentifier($this->generateTableName($csv)),
            $fileInfo['s3Path']['bucket'],
            $fileInfo['s3Path']['key'],
            implode(' ', $csvOptions),
            $this->quote($fileInfo['credentials']['AccessKeyId']),
            $this->quote($fileInfo['credentials']['SecretAccessKey']),
            $this->quote($fileInfo['credentials']['SessionToken'])
        );
    }

    /**
     * Create table from csv file with text columns
     *
     * @param CsvFile $file
     */
    protected function createTextTable(CsvFile $file)
    {
        $tableName = $this->generateTableName($file);

        $this->connection->query(sprintf(
            'DROP TABLE IF EXISTS %s',
            $this->connection->quoteIdentifier($tableName)
        ));

        $this->connection->query(sprintf(
            'CREATE TABLE %s (%s);',
            $this->connection->quoteIdentifier($tableName),
            implode(
                ', ',
                array_map(function ($column) {
                    $q = '"';
                    return ($q . str_replace("$q", "$q$q", $column) . $q) . ' VARCHAR NOT NULL';
                }, $file->getHeader())
            ),
            $tableName
        ));

        $storageApi = new Client([
            'token' => getenv('STORAGE_API_TOKEN')
        ]);

        $storageFileInfo = $storageApi->getFile(
            $storageApi->uploadFile(
                (string) $file,
                new FileUploadOptions()
            ),
            (new GetFileOptions())->setFederationToken(true)
        );

        $this->connection->query($this->generateCreateCommand($file, $storageFileInfo));

        $result = $this->connection->fetchAll(sprintf(
            'SELECT COUNT(*) AS %s FROM %s',
            $this->connection->quoteIdentifier('itemsCount'),
            $this->connection->quoteIdentifier($tableName)
        ));

        $this->assertEquals($this->countTable($file), (int) $result[0]['itemsCount']);
    }

    /**
     * Count records in CSV (with headers)
     *
     * @param CsvFile $file
     * @return int
     */
    protected function countTable(CsvFile $file)
    {
        $linesCount = 0;
        foreach ($file as $i => $line) {
            // skip header
            if (!$i) {
                continue;
            }

            $linesCount++;
        }

        return $linesCount;
    }
}
