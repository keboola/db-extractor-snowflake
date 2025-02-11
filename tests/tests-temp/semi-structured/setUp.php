<?php

declare(strict_types=1);

use Keboola\DbExtractor\FunctionalTests\DatabaseManager;
use Keboola\DbExtractor\FunctionalTests\DatadirTest;

return function (DatadirTest $test): void {
    $manager = new DatabaseManager($test->getConnection());

    $manager->createTable('semi-structured', [
        'var' => 'VARIANT',
        'obj' => 'OBJECT',
        'arr' => 'ARRAY',
    ]);

    $sql = <<<SQL
INSERT INTO "semi-structured" 
  SELECT 
      OBJECT_CONSTRUCT('a', 1, 'b', 'BBBB', 'c', null) AS "var",
      OBJECT_CONSTRUCT('a', 1, 'b', 'BBBB', 'c', null) AS "org",
      ARRAY_CONSTRUCT(10, 20, 30) AS "arr";
SQL;

    $test->getConnection()->query($sql);
};
