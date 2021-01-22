<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration\NodeDefinition;

use Keboola\DbExtractorConfig\Configuration\NodeDefinition\DbNode;
use Symfony\Component\Config\Definition\Builder\NodeBuilder;

class SnowflakeDbNode extends DbNode
{
    public const NODE_NAME = 'db';

    protected function init(NodeBuilder $builder): void
    {
        parent::init($builder);

        $this->addSchemaNode($builder);
        $this->addWarehouseNode($builder);
    }

    private function addSchemaNode(NodeBuilder $builder): void
    {
        $builder->scalarNode('schema');
    }

    private function addWarehouseNode(NodeBuilder $builder): void
    {
        $builder->scalarNode('warehouse');
    }
}
