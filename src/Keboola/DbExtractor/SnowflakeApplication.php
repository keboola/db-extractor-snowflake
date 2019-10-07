<?php

declare(strict_types=1);

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Configuration\NodeDefinition\SnowflakeDbNode;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractorConfig\Config;
use Keboola\DbExtractorConfig\Configuration\ConfigDefinition;
use Keboola\DbExtractorLogger\Logger;
use Keboola\DbExtractorConfig\Exception\UserException as ConfigUserException;

class SnowflakeApplication extends Application
{
    public function __construct(array $config, Logger $logger, array $state, string $dataDir)
    {
        $config['parameters']['data_dir'] = $dataDir;
        $config['parameters']['extractor_class'] = 'Snowflake';

        parent::__construct($config, $logger, $state);
    }

    protected function buildConfig(array $config): void
    {
        try {
            $this->config = new Config($config, new ConfigDefinition(new SnowflakeDbNode()));
        } catch (ConfigUserException $e) {
            throw new UserException($e->getMessage(), $e->getCode(), $e);
        }
    }
}
