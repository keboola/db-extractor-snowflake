<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Traits;

trait QuoteTrait
{
    public function quote(string $value): string
    {
        return "'" . addslashes($value) . "'";
    }
}
