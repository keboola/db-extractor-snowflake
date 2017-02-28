<?php
namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Configuration\SnowflakeConfigDefinition;

class SnowflakeApplication extends Application
{
	public function __construct(array $config, $dataDir)
	{
		$config['parameters']['data_dir'] = $dataDir;
		$config['parameters']['extractor_class'] = 'Snowflake';

		parent::__construct($config);

		$this->setConfigDefinition(new SnowflakeConfigDefinition());
	}
}