<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration;

use Keboola\DbExtractorConfig\Configuration\ConfigRowDefinition;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;

class SnowflakeConfigRowDefinition extends ConfigRowDefinition
{
    protected function getParametersDefinition(): ArrayNodeDefinition
    {
        $parametersNode = parent::getParametersDefinition();

        // @formatter:off
        $parametersNode
            ->children()
                ->booleanNode('propagateDescriptions')
                    ->defaultTrue();
        // @formatter:on

        return $parametersNode;
    }
}
