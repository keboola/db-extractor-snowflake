<?php
namespace Keboola\DbExtractor;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Snowflake\Connection;

class SnowflakeTest extends AbstractSnowflakeTest
{
	public function setUp()
	{
		if (!defined('APP_NAME')) {
			define('APP_NAME', 'ex-db-snowflake');
		}

		$config = $this->getConfig();

		$this->connection = new Connection($config['parameters']['db']);
	}

	public function testCredentials()
	{
		$config = $this->getConfig();
		$config['action'] = 'testConnection';
		unset($config['parameters']['tables']);

		$app = $this->createApplication($config);
		$result = $app->run();

		$this->assertArrayHasKey('status', $result);
		$this->assertEquals('success', $result['status']);
	}

	public function testRunWithoutTables()
	{
		$config = $this->getConfig();

		unset($config['parameters']['tables']);

		$app = $this->createApplication($config);
		$result = $app->run();

		$this->assertArrayHasKey('status', $result);
		$this->assertEquals('success', $result['status']);
	}

	public function testRun()
	{
		$config = $this->getConfig();

		$app = $this->createApplication($config);


		$csv1 = new CsvFile($this->dataDir . '/snowflake/sales.csv');
		$this->createTextTable($csv1);

		$csv2 = new CsvFile($this->dataDir . '/snowflake/escaping.csv');
		$this->createTextTable($csv2);

		$result = $app->run();


//		$outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv';

		$this->assertEquals('success', $result['status']);
//		$this->assertFileExists($outputCsvFile);
//		$this->assertFileExists($this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv.manifest');
//		$this->assertFileEquals((string) $csv1, $outputCsvFile);


//		$outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][1] . '.csv';

		$this->assertEquals('success', $result['status']);
//		$this->assertFileExists($outputCsvFile);
//		$this->assertFileExists($this->dataDir . '/out/tables/' . $result['imported'][1] . '.csv.manifest');
//		$this->assertFileEquals((string) $csv2, $outputCsvFile);
	}
}
