<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use ErrorException;
use Keboola\DbExtractor\Extractor\SnowflakeConnectionFactory;
use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use ReflectionMethod;
use Throwable;

class SnowflakeConnectionFactoryTest extends TestCase
{
    /**
     * A transient 5xx from the driver's REST layer used to escape the "DESC USER" lookup as a raw
     * ErrorException and end the job with an internal error. It is now retried.
     */
    public function testGetUserDefaultWarehouseRetriesTransientFailureThenSucceeds(): void
    {
        $factory = $this->createTestableFactory();
        // Two transient failures, then success on the third attempt.
        $factory->failuresBeforeSuccess = 2;

        $result = $this->invokeGetUserDefaultWarehouse($factory);

        $this->assertSame('MY_WAREHOUSE', $result);
        $this->assertSame(3, $factory->attemptCount);
    }

    /**
     * A 5xx that keeps repeating still fails, with the identical exception, so a persistent outage
     * ends the job exactly as it did before.
     */
    public function testGetUserDefaultWarehouseRethrowsSameExceptionAfterExhaustingAttempts(): void
    {
        $factory = $this->createTestableFactory();
        // Never succeeds, so every attempt throws.
        $factory->failuresBeforeSuccess = PHP_INT_MAX;

        try {
            $this->invokeGetUserDefaultWarehouse($factory);
            $this->fail('Expected exception was not thrown after exhausting warehouse lookup attempts');
        } catch (Throwable $e) {
            $this->assertInstanceOf(ErrorException::class, $e);
            $this->assertSame(TestableSnowflakeConnectionFactory::TRANSIENT_ERROR_MESSAGE, $e->getMessage());
        }

        $this->assertSame($factory->maxAttempts(), $factory->attemptCount);
    }

    /**
     * Anything other than a server-side 5xx must keep failing on the very first attempt, so no
     * failure that used to be reported immediately is now delayed by the retries.
     */
    public function testGetUserDefaultWarehouseDoesNotRetryNonTransientFailure(): void
    {
        $factory = $this->createTestableFactory();
        $factory->failuresBeforeSuccess = PHP_INT_MAX;
        $factory->errorMessage = TestableSnowflakeConnectionFactory::DETERMINISTIC_ERROR_MESSAGE;

        try {
            $this->invokeGetUserDefaultWarehouse($factory);
            $this->fail('Expected exception was not thrown');
        } catch (Throwable $e) {
            $this->assertSame(TestableSnowflakeConnectionFactory::DETERMINISTIC_ERROR_MESSAGE, $e->getMessage());
        }

        $this->assertSame(1, $factory->attemptCount);
    }

    /**
     * A user without a default warehouse still resolves to null on the first attempt - the retry
     * only ever triggers on a thrown error, never on a successful lookup.
     */
    public function testGetUserDefaultWarehouseReturnsNullWithoutRetryingWhenLookupSucceeds(): void
    {
        $factory = $this->createTestableFactory();
        $factory->warehouse = null;

        $this->assertNull($this->invokeGetUserDefaultWarehouse($factory));
        $this->assertSame(1, $factory->attemptCount);
    }

    /**
     * @dataProvider retryableWarehouseLookupErrorProvider
     */
    public function testIsRetryableWarehouseLookupError(string $message, bool $expected): void
    {
        $method = new ReflectionMethod(SnowflakeConnectionFactory::class, 'isRetryableWarehouseLookupError');
        $method->setAccessible(true);

        $this->assertSame(
            $expected,
            $method->invoke($this->createTestableFactory(), new ErrorException($message, 0, E_WARNING)),
        );
    }

    public function retryableWarehouseLookupErrorProvider(): array
    {
        return [
            'REST 503 that ended a job with an internal error' => [
                TestableSnowflakeConnectionFactory::TRANSIENT_ERROR_MESSAGE,
                true,
            ],
            'REST 502 from the driver' => [
                'odbc_exec(): SQL error: REST request failed: HTTP error (http error) - code=502',
                true,
            ],
            'missing or unauthorized user must keep failing immediately' => [
                TestableSnowflakeConnectionFactory::DETERMINISTIC_ERROR_MESSAGE,
                false,
            ],
            'a client-side 4xx is not retried' => [
                'odbc_exec(): SQL error: REST request failed: HTTP error (http error) - code=403',
                false,
            ],
            'bad credentials must keep failing immediately' => [
                '250001 (08001): Failed to connect to DB: Incorrect username or password was specified.',
                false,
            ],
            'a numeric error code that merely starts with 5 is not an HTTP status' => [
                '253002 (n/a): While getting file(s) there was an error',
                false,
            ],
            'empty message' => ['', false],
        ];
    }

    private function invokeGetUserDefaultWarehouse(TestableSnowflakeConnectionFactory $factory): ?string
    {
        $databaseConfig = $this->createMock(DatabaseConfig::class);
        $databaseConfig->method('getUsername')->willReturn('testuser');

        $method = new ReflectionMethod(SnowflakeConnectionFactory::class, 'getUserDefaultWarehouse');
        $method->setAccessible(true);

        /** @var string|null $result */
        $result = $method->invoke($factory, null, $databaseConfig);

        return $result;
    }

    private function createTestableFactory(): TestableSnowflakeConnectionFactory
    {
        return new TestableSnowflakeConnectionFactory(new NullLogger(), 5);
    }
}
