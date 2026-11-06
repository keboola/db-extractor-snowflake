<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Configuration\NodeDefinition\SnowflakeDbNode;
use Keboola\DbExtractor\Configuration\SnowflakeConfigRowDefinition;
use Keboola\DbExtractor\Configuration\SnowflakeTableNodeDecorator;
use Keboola\DbExtractorConfig\Config;
use PHPUnit\Framework\TestCase;

class ConfigTest extends TestCase
{
    public function testPropagateDescriptionsDefaultsToEnabled(): void
    {
        $config = new Config($this->getTableRowConfig(), $this->createRowDefinition());

        $this->assertTrue($config->getParameters()['propagateDescriptions']);
    }

    public function testPropagateDescriptionsCanBeDisabled(): void
    {
        $rawConfig = $this->getTableRowConfig();
        $rawConfig['parameters']['propagateDescriptions'] = false;

        $config = new Config($rawConfig, $this->createRowDefinition());

        $this->assertFalse($config->getParameters()['propagateDescriptions']);
    }

    private function createRowDefinition(): SnowflakeConfigRowDefinition
    {
        return new SnowflakeConfigRowDefinition(
            new SnowflakeDbNode(),
            null,
            null,
            new SnowflakeTableNodeDecorator(),
        );
    }

    private function getTableRowConfig(): array
    {
        return [
            'parameters' => [
                'db' => [
                    'host' => 'snowflake',
                    'user' => 'user',
                    '#password' => 'password',
                    'port' => 443,
                    'database' => 'DATABASE',
                    'schema' => 'SCHEMA',
                    'warehouse' => 'WAREHOUSE',
                ],
                'table' => [
                    'schema' => 'SCHEMA',
                    'tableName' => 'comments',
                ],
                'outputTable' => 'in.c-main.comments',
                'data_dir' => '/tmp/testDatadir',
                'extractor_class' => 'Snowflake',
            ],
        ];
    }
}
