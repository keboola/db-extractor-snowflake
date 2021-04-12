<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Adapter\ODBC\OdbcConnection;
use Keboola\DbExtractor\Configuration\ValueObject\SnowflakeDatabaseConfig;
use Psr\Log\LoggerInterface;

class SnowflakeOdbcConnection extends OdbcConnection
{

    public function __construct(
        LoggerInterface $logger,
        SnowflakeDatabaseConfig $databaseConfig,
        ?callable $init = null,
        int $connectMaxRetries = self::CONNECT_DEFAULT_MAX_RETRIES,
        int $odbcCursorType = SQL_CURSOR_FORWARD_ONLY,
        int $odbcCursorMode = SQL_CUR_USE_DRIVER
    ) {
        $dsn = $this->buildDsnString($databaseConfig);
        parent::__construct(
            $logger,
            $dsn,
            $databaseConfig->getUsername(),
            $databaseConfig->getPassword(),
            $init,
            $connectMaxRetries,
            $odbcCursorType,
            $odbcCursorMode
        );
    }

    public function quoteIdentifier(string $str): string
    {
        return '"' . str_replace('"', '""', $str) . '"';
    }

    protected function buildDsnString(SnowflakeDatabaseConfig $databaseConfig): string
    {
        $dsn = 'Driver=SnowflakeDSIIDriver;Server=' . $databaseConfig->getHost();
        $dsn .= ';Port=' . $databaseConfig->getPort();
        $dsn .= ';Tracing=0';
        $dsn .= ';Database=' . $this->quoteIdentifier($databaseConfig->getDatabase());

        if ($databaseConfig->hasSchema()) {
            $dsn .= ';Schema=' . $this->quoteIdentifier($databaseConfig->getSchema());
        }

        if ($databaseConfig->hasWarehouse()) {
            $dsn .= ';Warehouse=' . $this->quoteIdentifier($databaseConfig->getWarehouse());
        }

        return $dsn;
    }
}
