<?php
namespace Keboola\DbExtractor\Extractor;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Snowflake\Connection;

class Snowflake extends Extractor
{
	const STATEMENT_TIMEOUT_IN_SECONDS = 900; //@FIXME why?

	private $dbConfig;

	public function createConnection($dbParams)
	{
		$connection = new Connection($dbParams);
		$connection->query(sprintf("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = %d", self::STATEMENT_TIMEOUT_IN_SECONDS));
		return $connection;
	}

	private function restartConnection()
	{
		$this->db = null;
		try {
			$this->db = $this->createConnection($this->dbConfig);
		} catch (\Exception $e) {
			throw new UserException(sprintf("Error connecting to DB: %s", $e->getMessage()), 0, $e);
		}
	}

	public function export(array $table)
	{
		$outputTable = $table['outputTable'];

		$this->logger->info("Exporting to " . $outputTable);

		$query = $table['query'];

		$tries = 0;
		$exception = null;

		$csvCreated = false;
		while ($tries < 5) {
			$exception = null;
			try {
				if ($tries > 0) {
					$this->logger->info("Retrying query");
					$this->restartConnection();
				}
				$csvCreated = $this->executeQuery($query, $this->createOutputCsv($outputTable));
				break;
			} catch (\PDOException $e) {
				$exception = new UserException("DB query failed: " . $e->getMessage(), 0, $e);
			}

			sleep(pow($tries, 2));
			$tries++;
		}

		if ($exception) {
			throw $exception;
		}

		if ($csvCreated) {
			if ($this->createManifest($table) === false) {
				throw new ApplicationException("Unable to create manifest", 0, null, [
					'table' => $table
				]);
			}
		}

		return $outputTable;
	}

	protected function executeQuery($query, CsvFile $csv)
	{
		$cursorName = 'exdbcursor' . intval(microtime(true));

		$curSql = "DECLARE $cursorName CURSOR FOR $query";

		try {
			$this->db->beginTransaction(); // cursors require a transaction.
			$stmt = $this->db->prepare($curSql);
			$stmt->execute();
			$innerStatement = $this->db->prepare("FETCH 1 FROM $cursorName");
			$innerStatement->execute();

			// write header and first line
			$resultRow = $innerStatement->fetch(\PDO::FETCH_ASSOC);

			if (is_array($resultRow) && !empty($resultRow)) {
				$csv->writeRow(array_keys($resultRow));

				if (isset($this->dbConfig['replaceNull'])) {
					$resultRow = $this->replaceNull($resultRow, $this->dbConfig['replaceNull']);
				}
				$csv->writeRow($resultRow);

				// write the rest
				$innerStatement = $this->db->prepare("FETCH 5000 FROM $cursorName");

				while ($innerStatement->execute() && count($resultRows = $innerStatement->fetchAll(\PDO::FETCH_ASSOC)) > 0) {
					foreach ($resultRows as $resultRow) {
						if (isset($this->dbConfig['replaceNull'])) {
							$resultRow = $this->replaceNull($resultRow, $this->dbConfig['replaceNull']);
						}
						$csv->writeRow($resultRow);
					}
				}

				// close the cursor
				$this->db->exec("CLOSE $cursorName");
				$this->db->commit();

				return true;
			} else {
				$this->logger->warning("Query returned empty result. Nothing was imported.");

				return false;
			}
		} catch (\PDOException $e) {
			try {
				$this->db->rollBack();
			} catch (\Exception $e2) {

			}
			$innerStatement = null;
			$stmt = null;
			throw $e;
		}
	}

	private function replaceNull($row, $value)
	{
		foreach ($row as $k => $v) {
			if ($v === null) {
				$row[$k] = $value;
			}
		}

		return $row;
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
