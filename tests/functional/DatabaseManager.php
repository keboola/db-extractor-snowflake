<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\FunctionalTests;

use Keboola\DbExtractor\TraitTests\Tables\AutoIncrementTableTrait;
use Keboola\DbExtractor\TraitTests\Tables\EscapingTableTrait;
use Keboola\DbExtractor\TraitTests\Tables\SalesTableTrait;
use Keboola\DbExtractor\TraitTests\Tables\TypesTableTrait;
use Keboola\SnowflakeDbAdapter\Connection;

class DatabaseManager
{
    use AutoIncrementTableTrait;
    use SalesTableTrait;
    use EscapingTableTrait;
    use TypesTableTrait;

    protected Connection $connection;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
    }
}
