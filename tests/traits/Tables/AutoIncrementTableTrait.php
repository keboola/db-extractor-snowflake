<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TraitTests\Tables;

use Keboola\DbExtractor\TraitTests\AddConstraintTrait;
use Keboola\DbExtractor\TraitTests\CreateTableTrait;
use Keboola\DbExtractor\TraitTests\InsertRowsTrait;

trait AutoIncrementTableTrait
{
    use CreateTableTrait;
    use InsertRowsTrait;
    use AddConstraintTrait;

    public function createAITable(string $name = 'auto Increment Timestamp'): void
    {
        $this->createTable($name, $this->getAIColumns());
    }

    public function generateAIRows(string $tableName = 'auto Increment Timestamp'): void
    {
        $data = $this->getAIRows();
        $this->insertRows($tableName, $data['columns'], $data['data']);
    }

    private function getAIRows(): array
    {
        return [
            'columns' => ['Weir%d Na-me', 'type', 'someInteger', 'someDecimal', 'datetime'],
            'data' => [
                ['mario', 'plumber', 1, 1.1, '2021-01-01 13:43:17'],
                ['luigi', 'plumber', 2, 2.2, '2021-01-05 13:48:17'],
                ['toad', 'mushroom', 3, 3.3, '2021-01-03 13:43:17'],
                ['princess', 'royalty', 4, 4.4, '2021-01-05 13:41:17'],
                ['wario', 'badguy', 5, 5.5, '2021-01-04 13:43:17'],
                ['yoshi', 'horse?', 6, 6.6, '2021-01-05 13:43:27'],
            ],
        ];
    }

    private function getAIColumns(): array
    {
        return [
            '_Weir%d I-D' => 'INT NOT NULL AUTOINCREMENT PRIMARY KEY',
            'Weir%d Na-me' => 'VARCHAR(55) NOT NULL DEFAULT \'mario\'',
            'someInteger' => 'INT',
            'someDecimal' => 'DECIMAL(10,8) NOT NULL DEFAULT 0.0',
            'type' => 'VARCHAR(55) NULL',
            'datetime' => 'DATETIME NOT NULL DEFAULT to_timestamp_ntz(current_timestamp())',
        ];
    }
}
