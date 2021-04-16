<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Adapter\ODBC\OdbcConnection;

class SnowflakeOdbcConnection extends OdbcConnection
{
    use QuoteTrait;

    public function testConnection(): void
    {
        $this->query('SELECT current_date;');
    }
}
