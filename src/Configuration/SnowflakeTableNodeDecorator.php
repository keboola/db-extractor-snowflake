<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration;

use Keboola\DbExtractorConfig\Configuration\NodeDefinition\TableNodesDecorator;

class SnowflakeTableNodeDecorator extends TableNodesDecorator
{
    public function normalize(array $v): array
    {
        // Fix BC: some old configs can contain limit = 0
        if (empty($v['incrementalFetchingLimit'])) {
            unset($v['incrementalFetchingLimit']);
        }

        return parent::normalize($v);
    }
}
