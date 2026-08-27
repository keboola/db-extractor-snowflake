<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use InvalidArgumentException;
use Keboola\DbExtractor\Configuration\ValueObject\SnowflakeDatabaseConfig;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\SnowflakeDbAdapter\Builder\DSNBuilder;
use Keboola\SnowflakeDbAdapter\Connection;
use Keboola\SnowflakeDbAdapter\Exception\CannotAccessObjectException;
use Psr\Log\LoggerInterface;
use Retry\BackOff\BackOffPolicyInterface;
use Retry\BackOff\ExponentialBackOffPolicy;
use Retry\Policy\CallableRetryPolicy;
use Retry\RetryProxy;
use Throwable;

class SnowflakeConnectionFactory
{
    use QuoteTrait;

    private LoggerInterface $logger;

    private int $maxRetries;

    private const SNOWFLAKE_APPLICATION = 'Keboola_Connection';

    /**
     * Number of attempts for the "DESC USER" default-warehouse lookup.
     * The lookup is a read-only statement, so repeating it has no side effects.
     */
    protected const WAREHOUSE_LOOKUP_MAX_ATTEMPTS = 3;

    /**
     * The Snowflake ODBC driver reports a failed HTTP call of its REST layer with the status code
     * inlined in the message, e.g. "REST request failed: HTTP error (http error) - code=503 Verify
     * that the hostnames and portnumbers in SYSTEM$ALLOWLIST are added to your firewall's allowed
     * list." A 5xx comes from Snowflake (or the network in between), never from the statement
     * itself, so the very same lookup is worth trying again.
     */
    private const WAREHOUSE_LOOKUP_RETRYABLE_PATTERN = '~\bcode=5\d\d\b~';

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
        $options = [
            'host' => $databaseConfig->getHost(),
            'user' => $databaseConfig->getUsername(),
            'password' => $databaseConfig->getPassword(),
            'port' => $databaseConfig->getPort(),
            'database' => $databaseConfig->getDatabase(),
            'application' => self::SNOWFLAKE_APPLICATION,
        ];

        if ($databaseConfig->hasSchema()) {
            $options['schema'] = $databaseConfig->getSchema();
        }

        if ($databaseConfig->hasWarehouse()) {
            $options['warehouse'] = $databaseConfig->getWarehouse();
        }

        if ($databaseConfig->hasRoleName()) {
            $options['roleName'] = $databaseConfig->getRoleName();
        }

        if ($databaseConfig->hasPrivateKey()) {
            $options['privateKey'] = $databaseConfig->getPrivateKey();
        }

        return DSNBuilder::build($options);
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
                'Please configure "warehouse" parameter. User default warehouse is not defined.',
            );
        }

        try {
            odbc_exec($connection, sprintf(
                'USE WAREHOUSE %s;',
                $this->quoteIdentifier($warehouse),
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
                $this->quoteIdentifier($databaseConfig->getSchema()),
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
                json_encode(['runId' => $runId]),
            ));
        }
    }

    /**
     * @param resource $connection
     */
    protected function getUserDefaultWarehouse($connection, DatabaseConfig $databaseConfig): ?string
    {
        // "DESC USER" only reads metadata, so repeating it is side-effect free. This exists purely
        // to smooth over a transient 5xx from the driver's REST layer, which used to escape as a
        // raw ErrorException and end the job with an opaque internal error. A lookup that succeeds
        // on the first attempt behaves exactly as before, and any other failure - or a 5xx that
        // keeps repeating - still throws the identical exception, unchanged.
        $warehouse = $this->createWarehouseLookupRetryProxy()->call(
            fn (): ?string => $this->readUserDefaultWarehouse($connection, $databaseConfig),
        );
        assert($warehouse === null || is_string($warehouse));

        return $warehouse;
    }

    protected function createWarehouseLookupRetryProxy(): RetryProxy
    {
        return new RetryProxy(
            new CallableRetryPolicy(
                fn (Throwable $e): bool => $this->isRetryableWarehouseLookupError($e),
                self::WAREHOUSE_LOOKUP_MAX_ATTEMPTS,
            ),
            $this->createWarehouseLookupBackOffPolicy(),
            $this->logger,
        );
    }

    protected function createWarehouseLookupBackOffPolicy(): BackOffPolicyInterface
    {
        return new ExponentialBackOffPolicy(1000);
    }

    /**
     * Only a server-side 5xx reported by the driver's REST layer is retried - see
     * WAREHOUSE_LOOKUP_RETRYABLE_PATTERN. Every other failure (a missing user, an insufficient
     * role, bad credentials, ...) keeps failing on the first attempt, exactly as before, so
     * nothing that used to be reported immediately is now delayed by the retries.
     */
    protected function isRetryableWarehouseLookupError(Throwable $e): bool
    {
        return (bool) preg_match(self::WAREHOUSE_LOOKUP_RETRYABLE_PATTERN, $e->getMessage());
    }

    /**
     * @param resource $connection
     */
    protected function readUserDefaultWarehouse($connection, DatabaseConfig $databaseConfig): ?string
    {
        $stmt = odbc_exec($connection, sprintf(
            'DESC USER %s;',
            $this->quoteIdentifier($databaseConfig->getUsername()),
        ));

        while ($item = odbc_fetch_array($stmt)) {
            if ($item['property'] === 'DEFAULT_WAREHOUSE') {
                return $item['value'] === 'null' ? null : $item['value'];
            }
        }

        return null;
    }
}
