<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TraitTests;

use Keboola\SnowflakeDbAdapter\Connection;

trait RemoveAllTablesTrait
{
    use QuoteIdentifierTrait;

    protected Connection $connection;

    protected function removeAllTables(): void
    {
        $views = $this->connection->fetchAll('SHOW TABLES IN SCHEMA');
        foreach ($views as $view) {
            $this->connection->query(
                sprintf(
                    'DROP TABLE IF EXISTS %s.%s',
                    $this->quoteIdentifier($view['schema_name']),
                    $this->quoteIdentifier($view['name'])
                )
            );
        }
        $views = $this->connection->fetchAll('SHOW VIEWS IN SCHEMA');
        foreach ($views as $view) {
            $this->connection->query(
                sprintf(
                    'DROP VIEW IF EXISTS %s.%s',
                    $this->quoteIdentifier($view['schema_name']),
                    $this->quoteIdentifier($view['name'])
                )
            );
        }
    }
}
