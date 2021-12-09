<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Adapter\Exception\OdbcException;
use Keboola\DbExtractor\Adapter\ODBC\OdbcConnection;
use Throwable;

class SnowflakeOdbcConnection extends OdbcConnection
{
    use QuoteTrait;

    protected function connect(): void
    {
        $this->logger->info(sprintf('Creating ODBC connection to "%s".', $this->dsn));
        ini_set('odbc.default_cursortype', (string) $this->odbcCursorType);
        try {
            /** @var resource|false $connection */
            $connection = @odbc_connect($this->dsn, $this->user, $this->password, $this->odbcCursorMode);
        } catch (Throwable $e) {
            throw new OdbcException($e->getMessage(), $e->getCode(), $e);
        }

        // "odbc_connect" can generate warning, if "set_error_handler" is not set, so we are checking it manually
        if ($connection === false) {
            if (strpos(odbc_errormsg(), 'msg=')) {
                preg_match('/msg=\'(.*)\'\./', odbc_errormsg(), $message);
                throw new OdbcException($message[1] . ' ' . odbc_error());
            }
            throw new OdbcException(odbc_errormsg() . ' ' . odbc_error());
        }

        if ($this->init) {
            ($this->init)($connection);
        }

        $this->connection = $connection;
    }

    public function testConnection(): void
    {
        $this->query('SELECT current_date;');
    }
}
