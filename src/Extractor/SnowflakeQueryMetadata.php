<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Adapter\ValueObject\QueryMetadata;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\ColumnCollection;

class SnowflakeQueryMetadata implements QueryMetadata
{
    private ColumnCollection $columns;

    public function __construct(ColumnCollection $columns)
    {
        $this->columns = $columns;
    }

    public function getColumns(): ColumnCollection
    {
        return $this->columns;
    }
}
