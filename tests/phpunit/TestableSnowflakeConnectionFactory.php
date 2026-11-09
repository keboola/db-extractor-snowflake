<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use ErrorException;
use Keboola\DbExtractor\Extractor\SnowflakeConnectionFactory;
use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use Retry\BackOff\BackOffPolicyInterface;
use Retry\BackOff\NoBackOffPolicy;

/**
 * Test double exercising the "DESC USER" default-warehouse lookup retry in
 * SnowflakeConnectionFactory without a real ODBC connection or a back-off sleep. The retry policy
 * itself is NOT overridden, so the tests run against the production attempt count and the
 * production retryable-error predicate.
 */
class TestableSnowflakeConnectionFactory extends SnowflakeConnectionFactory
{
    /** The exact message the Snowflake ODBC driver produced in the failure this retry addresses. */
    public const TRANSIENT_ERROR_MESSAGE = 'odbc_exec(): SQL error: REST request failed: HTTP error '
        . '(http error) - code=503 Verify that the hostnames and portnumbers in SYSTEM$ALLOWLIST '
        . 'are added to your firewall\'s allowed list., SQL state HY000 in SQLExecDirect';

    public const DETERMINISTIC_ERROR_MESSAGE = 'odbc_exec(): SQL error: SQL compilation error: '
        . "User 'NO_SUCH_USER' does not exist or not authorized., SQL state 37000 in SQLExecDirect";

    public int $attemptCount = 0;

    /** Number of leading attempts that throw before one succeeds. */
    public int $failuresBeforeSuccess = 0;

    /** Message the simulated failures carry; drives the production retryable-error predicate. */
    public string $errorMessage = self::TRANSIENT_ERROR_MESSAGE;

    /** Value the lookup resolves to once an attempt succeeds. */
    public ?string $warehouse = 'MY_WAREHOUSE';

    public function maxAttempts(): int
    {
        return self::WAREHOUSE_LOOKUP_MAX_ATTEMPTS;
    }

    protected function createWarehouseLookupBackOffPolicy(): BackOffPolicyInterface
    {
        // Same attempt count as production, but no back-off sleep so the test stays fast.
        return new NoBackOffPolicy();
    }

    /**
     * @param resource $connection
     * @throws ErrorException
     */
    protected function readUserDefaultWarehouse($connection, DatabaseConfig $databaseConfig): ?string
    {
        $this->attemptCount++;
        if ($this->attemptCount <= $this->failuresBeforeSuccess) {
            // odbc_exec() raises a PHP warning, which BaseComponent's error handler turns into an
            // ErrorException - the exact exception that used to end the job with an internal error.
            throw new ErrorException($this->errorMessage, 0, E_WARNING);
        }

        return $this->warehouse;
    }
}
