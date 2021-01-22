<?php

declare(strict_types=1);

use Keboola\DbExtractor\FunctionalTests\DatadirTest;
use Keboola\DbExtractor\FunctionalTests\DatabaseManager;

return function (DatadirTest $test): void {
    $manager = new DatabaseManager($test->getConnection());

    // escaping table
    $manager->createTypesTable('badTypes', [
        'character' => 'VARCHAR(100) NOT NULL',
        'integer' => 'VARCHAR(100) NOT NULL',
        'decimal' => 'VARCHAR(100) NOT NULL',
        'date' => 'VARCHAR(100) NOT NULL',
    ]);
    $manager->generateTypesRows('badTypes');

    $manager->insertRows(
        'badTypes',
        ['character', 'integer', 'decimal', 'date'],
        [
            ['abcdefgh', 32, '22.41000000.234','2017-08-07'],
        ]
    );
};
