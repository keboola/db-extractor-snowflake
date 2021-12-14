<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Throwable;
use Psr\Log\LoggerInterface;
use InvalidArgumentException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Configuration\ValueObject\SnowflakeDatabaseConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\SnowflakeDbAdapter\Exception\CannotAccessObjectException;

class SnowflakeConnectionFactory
{
    use QuoteTrait;

    private LoggerInterface $logger;

    private int $maxRetries;

    public function __construct(LoggerInterface $logger, int $maxRetries)
    {
        $this->logger = $logger;
        $this->maxRetries = $maxRetries;
    }

    public function create(DatabaseConfig $databaseConfig): SnowflakeOdbcConnection
    {
        if (!$databaseConfig instanceof SnowflakeDatabaseConfig) {
            throw new InvalidArgumentException('Instance of SnowflakeDatabaseConfig expected.');
        }

        try {
            return $this->doCreate($databaseConfig);
        } catch (CannotAccessObjectException $e) {
            throw new UserException($e->getMessage(), 0, $e);
        }
    }

    protected function doCreate(SnowflakeDatabaseConfig $databaseConfig): SnowflakeOdbcConnection
    {
        return new SnowflakeOdbcConnection(
            $this->logger,
            $this->buildDsnString($databaseConfig),
            $databaseConfig->getUsername(),
            $databaseConfig->getPassword(true),
            $this->getInitCallback($databaseConfig),
            $this->maxRetries,
        );
    }

    protected function getInitCallback(SnowflakeDatabaseConfig $databaseConfig): callable
    {
        return function ($connection) use ($databaseConfig): void {
            $this->setWarehouse($connection, $databaseConfig);
            $this->setSchema($connection, $databaseConfig);
            $this->setQueryTag($connection);
        };
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

    /**
     * @param resource $connection
     */
    protected function setWarehouse($connection, SnowflakeDatabaseConfig $databaseConfig): void
    {
        $warehouse = $databaseConfig->hasWarehouse() ?
            $databaseConfig->getWarehouse() :
            $this->getUserDefaultWarehouse($connection, $databaseConfig);

        if (!$warehouse) {
            throw new UserException(
                'Please configure "warehouse" parameter. User default warehouse is not defined.'
            );
        }

        try {
            odbc_exec($connection, sprintf(
                'USE WAREHOUSE %s;',
                $this->quoteIdentifier($warehouse)
            ));
        } catch (Throwable $e) {
            if (preg_match('/Object does not exist/ui', $e->getMessage())) {
                throw new UserException(sprintf('Invalid warehouse "%s" specified', $warehouse));
            } else {
                throw $e;
            }
        }
    }

    /**
     * @param resource $connection
     */
    protected function setSchema($connection, SnowflakeDatabaseConfig $databaseConfig): void
    {
        if ($databaseConfig->hasSchema()) {
            odbc_exec($connection, sprintf(
                'USE SCHEMA %s.%s',
                $this->quoteIdentifier($databaseConfig->getDatabase()),
                $this->quoteIdentifier($databaseConfig->getSchema())
            ));
        }
    }

    /**
     * @param resource $connection
     */
    protected function setQueryTag($connection): void
    {
        $runId = (string) getenv('KBC_RUNID');
        if ($runId) {
            odbc_exec($connection, sprintf(
                "ALTER SESSION SET QUERY_TAG='%s';",
                json_encode(['runId' => $runId])
            ));
        }
    }

    /**
     * @param resource $connection
     */
    protected function getUserDefaultWarehouse($connection, DatabaseConfig $databaseConfig): ?string
    {
        $stmt = odbc_exec($connection, sprintf(
            'DESC USER %s;',
            $this->quoteIdentifier($databaseConfig->getUsername())
        ));

        while ($item = odbc_fetch_array($stmt)) {
            if ($item['property'] === 'DEFAULT_WAREHOUSE') {
                return $item['value'] === 'null' ? null : $item['value'];
            }
        }

        return null;
    }
}
