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

    /**
     * @param array<string, mixed> $column
     */
    public static function getColumnLength(array $column): ?string
    {
        if (!empty($column['CHARACTER_MAXIMUM_LENGTH'])) {
            return (string) $column['CHARACTER_MAXIMUM_LENGTH'];
        }

        if (!is_null($column['NUMERIC_PRECISION'])) {
            if (is_numeric($column['NUMERIC_SCALE'])) {
                return (string) $column['NUMERIC_PRECISION'] . ',' . (string) $column['NUMERIC_SCALE'];
            }
            return (string) $column['NUMERIC_PRECISION'];
        }

        if (!is_null($column['DATETIME_PRECISION'])
            && self::isTemporalTypeWithPrecision((string) $column['DATA_TYPE'])
        ) {
            return (string) $column['DATETIME_PRECISION'];
        }

        return null;
    }

    private static function isTemporalTypeWithPrecision(string $dataType): bool
    {
        // Snowflake TIMESTAMP_NTZ/_LTZ/_TZ and TIME carry a fractional-seconds precision (0-9).
        // DATE intentionally excluded: it has no precision and php-datatypes rejects a length for it.
        return str_starts_with($dataType, 'TIMESTAMP') || $dataType === 'TIME';
    }
}
