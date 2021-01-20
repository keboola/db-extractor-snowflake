<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration\ValueObject;

use Keboola\DbExtractorConfig\Configuration\ValueObject\IncrementalFetchingConfig;

class SnowflakeIncrementalFetchingConfig extends IncrementalFetchingConfig
{
    public static function fromArray(array $data): ?IncrementalFetchingConfig
    {
        $data['incrementalFetchingLimit'] =
            !empty($data['incrementalFetchingLimit']) ?
                $data['incrementalFetchingLimit'] :
                null;

        return parent::fromArray($data);
    }
}
