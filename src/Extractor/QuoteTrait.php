<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

trait QuoteTrait
{
    public function quote(string $str): string
    {
        return "'" . str_replace("'", "''", $str) . "'";
    }

    public function quoteIdentifier(string $str): string
    {
        return '"' . str_replace('"', '""', $str) . '"';
    }
}
