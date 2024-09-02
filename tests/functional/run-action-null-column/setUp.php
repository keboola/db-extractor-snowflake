<?php

declare(strict_types=1);

use Keboola\DbExtractor\FunctionalTests\DatabaseManager;
use Keboola\DbExtractor\FunctionalTests\DatadirTest;

return function (DatadirTest $test): void {
    $test->getConnection()->query('CREATE OR REPLACE VIEW "null column table" AS SELECT NULL "test";');
};
