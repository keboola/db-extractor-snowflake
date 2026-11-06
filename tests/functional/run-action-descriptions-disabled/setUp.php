<?php

declare(strict_types=1);

use Keboola\DbExtractor\FunctionalTests\DatabaseManager;
use Keboola\DbExtractor\FunctionalTests\DatadirTest;

return function (DatadirTest $test): void {
    $manager = new DatabaseManager($test->getConnection());

    // Table carrying a table level COMMENT and column level COMMENTs
    $manager->createCommentsTable();
    $manager->generateCommentsRows();
};
