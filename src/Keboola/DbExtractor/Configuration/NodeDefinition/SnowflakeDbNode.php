<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration\NodeDefinition;

use Keboola\DbExtractorConfig\Configuration\NodeDefinition\DbNode;
use Keboola\DbExtractorConfig\Configuration\NodeDefinition\SshNode;
use Symfony\Component\Config\Definition\Builder\NodeParentInterface;

class SnowflakeDbNode extends DbNode
{
    public const NODE_NAME = 'db';

    public function __construct(?SshNode $sshNode = null, ?NodeParentInterface $parent = null)
    {
        parent::__construct($sshNode, $parent);
        $this->init();
    }

    protected function init(): void
    {
        // @formatter:off
        $this
            ->children()
                ->scalarNode('driver')->end()
                ->scalarNode('host')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->scalarNode('port')->end()
                ->scalarNode('warehouse')->end()
                ->scalarNode('database')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->scalarNode('schema')->end()
                ->scalarNode('user')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->scalarNode('#password')->end()
                ->append($this->sshNode)
            ->end()
        ;
        // @formatter:on
    }
}
