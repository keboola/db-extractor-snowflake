<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

class SnowflakeUtils
{
    public static function parseTypeAndLength(string $rawType): array
    {
        // Eg. NUMBER(38,0) / DATE / VARCHAR(16777216)
        preg_match('~^([^()]+)(?:\((.+)\))?~', $rawType, $m);
        $type = $m[1] ?? null;
        $length = $m[2] ?? null;
        return [$type, $length];
    }
}
