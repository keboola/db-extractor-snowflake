<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Exception;
use Keboola\DbExtractor\Extractor\SnowsqlExportAdapter;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Retry\BackOff\BackOffPolicyInterface;
use Retry\BackOff\NoBackOffPolicy;
use Symfony\Component\Process\Process;

/**
 * Test double exercising the download retry logic in SnowsqlExportAdapter without spawning a real
 * snowsql process or sleeping between attempts. The retry policy itself is NOT overridden, so the
 * tests run against the production attempt count and the production retryable-error predicate.
 */
class TestableSnowsqlExportAdapter extends SnowsqlExportAdapter
{
    public int $attemptCount = 0;

    /** Number of leading attempts that throw before one succeeds. */
    public int $failuresBeforeSuccess = 0;

    /** Whether the simulated failures look like a retryable blob storage read error. */
    public bool $simulateRetryableError = true;

    public function maxAttempts(): int
    {
        return self::DOWNLOAD_MAX_ATTEMPTS;
    }

    protected function createDownloadBackOffPolicy(): BackOffPolicyInterface
    {
        // Same attempt count as production, but no back-off sleep so the test stays fast.
        return new NoBackOffPolicy();
    }

    protected function runDownloadCommandOnce(ExportConfig $exportConfig, string $csvFilePath): Process
    {
        $this->attemptCount++;
        if ($this->attemptCount <= $this->failuresBeforeSuccess) {
            $this->downloadErrorIsRetryable = $this->simulateRetryableError;
            throw new Exception(sprintf(
                'File download error occurred processing [%s]',
                $exportConfig->getOutputTable(),
            ));
        }

        return Process::fromShellCommandline('true');
    }
}
