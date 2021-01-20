<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration\ValueObject;

use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\InputTable;

class SnowflakeExportConfig extends ExportConfig
{
    public static function fromArray(array $data): self
    {
        return new self(
            $data['id'] ?? null,
            $data['name'] ?? null,
            $data['query'],
            empty($data['query']) ? InputTable::fromArray($data) : null,
            $data['incremental'] ?? false,
            empty($data['query']) ? SnowflakeIncrementalFetchingConfig::fromArray($data) : null,
            $data['columns'],
            $data['outputTable'],
            $data['primaryKey'],
            $data['retries']
        );
    }
}
