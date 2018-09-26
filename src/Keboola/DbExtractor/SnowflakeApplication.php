<?php
namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Configuration\SnowflakeConfigDefinition;

class SnowflakeApplication extends Application
{
    public function __construct(array $config, Logger $logger, array $state, string $dataDir)
    {
        $config['parameters']['data_dir'] = $dataDir;
        $config['parameters']['extractor_class'] = 'Snowflake';

        parent::__construct($config, $logger, $state);

        $this->setConfigDefinition(new SnowflakeConfigDefinition());
    }
}
