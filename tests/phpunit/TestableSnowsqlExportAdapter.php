<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Exception;
use Keboola\DbExtractor\Extractor\SnowsqlExportAdapter;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Retry\BackOff\NoBackOffPolicy;
use Retry\Policy\SimpleRetryPolicy;
use Retry\RetryProxy;
use Symfony\Component\Process\Process;

/**
 * Test double exercising the download retry logic in SnowsqlExportAdapter without
 * spawning a real snowsql process or sleeping between attempts.
 */
class TestableSnowsqlExportAdapter extends SnowsqlExportAdapter
{
    public int $attemptCount = 0;

    /** Number of leading attempts that throw before one succeeds. */
    public int $failuresBeforeSuccess = 0;

    public function maxAttempts(): int
    {
        return self::DOWNLOAD_MAX_ATTEMPTS;
    }

    protected function createDownloadRetryProxy(): RetryProxy
    {
        // Same attempt count as production, but no back-off sleep so the test stays fast.
        return new RetryProxy(
            new SimpleRetryPolicy(self::DOWNLOAD_MAX_ATTEMPTS),
            new NoBackOffPolicy(),
        );
    }

    protected function runDownloadCommandOnce(ExportConfig $exportConfig, string $csvFilePath): Process
    {
        $this->attemptCount++;
        if ($this->attemptCount <= $this->failuresBeforeSuccess) {
            throw new Exception(sprintf(
                'File download error occurred processing [%s]',
                $exportConfig->getOutputTable(),
            ));
        }

        return Process::fromShellCommandline('true');
    }
}
