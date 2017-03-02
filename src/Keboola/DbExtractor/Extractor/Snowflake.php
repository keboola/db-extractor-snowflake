<?php
namespace Keboola\DbExtractor\Extractor;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Logger;
use Keboola\DbExtractor\Snowflake\Connection;
use Keboola\Temp\Temp;
use Symfony\Component\Process\Process;
use Symfony\Component\Yaml\Yaml;

class Snowflake extends Extractor
{
	const STATEMENT_TIMEOUT_IN_SECONDS = 900; //@FIXME why?

	private $dbConfig;

	/**
	 * @var Connection
	 */
	protected $db;

	/**
	 * @var \SplFileInfo
	 */
	private $snowSqlConfig;

	private $warehouse;
	private $database;
	private $schema;

	/**
	 * @var Temp
	 */
	private $temp;

	public function __construct($parameters, Logger $logger)
	{
		$this->temp = new Temp('ex-snowflake');

		parent::__construct($parameters, $logger);
	}

	public function createConnection($dbParams)
	{
		$this->snowSqlConfig = $this->crateSnowSqlConfig($dbParams);

		$connection = new Connection($dbParams);
		$connection->query(sprintf("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = %d", self::STATEMENT_TIMEOUT_IN_SECONDS));

		$this->database = $dbParams['database'];
		$this->schema = $dbParams['schema'];

		return $connection;
	}

	private function quote($value)
	{
		return "'" . addslashes($value) . "'";
	}


	/***
	 * Create stage in snowflake
	 */
	private function createStage()
	{
		$this->logger->info("Snowflake stage for export prepare: start");

		$sql = sprintf(
			"
			CREATE OR REPLACE STAGE %s;
			",
			$this->generateStageName()
		);

		$this->logger->debug(trim($sql));
		$this->db->query($sql);

		$this->logger->info("Snowflake stage for export prepare: end");
	}

	/***
	 * Drop stage in snowflake
	 */
	private function dropStage()
	{
		$this->logger->info("Snowflake stage for export drop: start");

		$sql = sprintf(
			"
			DROP STAGE IF EXISTS %s;
			",
			$this->generateStageName()
		);

		$this->logger->debug(trim($sql));
		$this->db->query($sql);

		$this->logger->info("Snowflake stage for export drop: end");
	}

	/**
	 * @param $output
	 * @param $path
	 * @return \SplFileInfo[]
	 */
	private function parseFiles($output, $path)
	{
		$files = [];
		$lines = explode("\n", $output);

		foreach ($lines as $line) {
			$matches = [];
			if (preg_match('/\| ([a-z0-9\_\-\.]+\.gz) \|/ui', $line, $matches) && preg_match('/ downloaded /ui', $line)) {
				$file = new \SplFileInfo($path . '/' . $matches[1]);
				if ($file->isFile()) {
					$files[] = $file;
				} else {
					//@FIXME maybe exception ?
				}
			}
		}

		return $files;
	}

	private function exportAndDownload(array $table)
	{
		$this->logger->info("Snowflake copy data to stage: start");

		$csvOptions = [];
		$csvOptions[] = sprintf('FIELD_DELIMITER = %s', $this->quote(CsvFile::DEFAULT_DELIMITER));
		$csvOptions[] = sprintf("FIELD_OPTIONALLY_ENCLOSED_BY = %s", $this->quote(CsvFile::DEFAULT_ENCLOSURE));
		$csvOptions[] = sprintf("ESCAPE_UNENCLOSED_FIELD = %s", $this->quote('\\'));

		$sql = sprintf(
			"
			COPY INTO @%s/%s
			FROM (%s)
			
			FILE_FORMAT = (TYPE=CSV %s)
			HEADER = true
			;
			",
			$this->generateStageName(),
			str_replace('.', '_', $table['outputTable']),
			$table['query'],
			implode(' ', $csvOptions)
		);

		$this->logger->debug(trim($sql));
		$this->db->query($sql);

		$this->logger->info("Snowflake stage for export drop: end");

		$this->logger->info("Snowflake get data: start");

		@mkdir($this->dataDir . '/out/tables', 0770, true);

		$sql = [];
		$sql[] = sprintf('USE DATABASE %s;', $this->db->quoteIdentifier($this->database));
		$sql[] = sprintf('USE SCHEMA %s;', $this->db->quoteIdentifier($this->schema));
		$sql[] = sprintf(
			'GET @%s/%s file://%s;',
			$this->generateStageName(),
			str_replace('.', '_', $table['outputTable']),
			$this->dataDir . '/out/tables/'
		);


		$snowSql = $this->temp->createTmpFile('snowsql.sql');
		file_put_contents($snowSql, implode("\n", $sql));

		$this->logger->debug(trim(implode("\n", $sql)));

		// execute external
		$command = sprintf(
			"snowsql --noup --config %s -c downloader -f %s",
			$this->snowSqlConfig,
			$snowSql
		);

		$this->logger->debug(trim($command));


		$process = new Process($command, null, null, null, self::STATEMENT_TIMEOUT_IN_SECONDS);
		$process->run();

		if (!$process->isSuccessful()) {
			throw new \Exception("File download error occured");
		}

		$csvFiles = $this->parseFiles($process->getOutput(), $this->dataDir . '/out/tables');
		foreach ($csvFiles AS $csvFile) {
			$manifestData = [
				'destination' => $table['outputTable'],
				'delimiter' => CsvFile::DEFAULT_DELIMITER,
				'enclosure' => CsvFile::DEFAULT_ENCLOSURE,
				'primary_key' => $table['primaryKey'],
				'incremental' => $table['incremental'],

			];

			file_put_contents($csvFile . '.manifest', Yaml::dump($manifestData));
		}
	}

	private function generateStageName()
	{
		return 'snowExRunId_' . str_replace('.', '_', getenv('KBC_RUNID'));
	}

	/**
	 * @param $dbParams
	 * @return \SplFileInfo
	 */
	private function crateSnowSqlConfig($dbParams)
	{
		$hostParts = explode('.', $dbParams['host']);

		$cliConfig[] = '';
		$cliConfig[] = '[options]';
		$cliConfig[] = 'exit_on_error = true';
		$cliConfig[] = '';
		$cliConfig[] = '[connections.downloader]';
		$cliConfig[] = sprintf('accountname = %s', reset($hostParts));
		$cliConfig[] = sprintf('username = %s', $dbParams['user']);
		$cliConfig[] = sprintf('password = %s', $dbParams['password']);
		$cliConfig[] = sprintf('dbname = %s', $dbParams['database']);
		$cliConfig[] = sprintf('schemaname = %s', $dbParams['schema']);
		//$cliConfig[] = sprintf('warehousename = %s', $dbParams['user']);

		$file = $this->temp->createFile('snowsql.config');;
		file_put_contents($file, implode("\n", $cliConfig));

		return $file;
	}

	public function export(array $table)
	{
		$outputTable = $table['outputTable'];

		$this->logger->info("Exporting to " . $outputTable);


		$this->createStage(); //@FIXME for all


		$this->exportAndDownload($table);

		$this->dropStage(); //@FIXME for all

//		if ($csvCreated) {
//			if ($this->createManifest($table) === false) {
//				throw new ApplicationException("Unable to create manifest", 0, null, [
//					'table' => $table
//				]);
//			}
//		}

		return $outputTable;
	}

	protected function executeQuery($query, CsvFile $csv)
	{
	}

	public function testConnection()
	{
		$this->execQuery('SELECT current_date;');
	}

	private function execQuery($query)
	{
		$this->logger->info(sprintf("Executing query '%s'", $this->hideCredentialsInQuery($query)));
		try {
			$this->db->query($query);
		} catch (\Exception $e) {
			throw new UserException("Query execution error: " . $e->getMessage(), 0, $e);
		}
	}

	private function hideCredentialsInQuery($query)
	{
		return preg_replace("/(AWS_[A-Z_]*\\s=\\s.)[0-9A-Za-z\\/\\+=]*./", '${1}...\'', $query);
	}

}
