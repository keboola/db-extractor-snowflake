<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Utils;

class AccountUrlParser
{
    public static function parse(string $host): string
    {
        $hostParts = explode('.', $host);
        return implode('.', array_slice($hostParts, 0, count($hostParts) - 2));
    }
}
