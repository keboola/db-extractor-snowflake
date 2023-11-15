<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TraitTests;

use Keboola\SnowflakeDbAdapter\Connection;

trait CreateTableTrait
{
    use QuoteIdentifierTrait;

    protected Connection $connection;

    public function createTable(string $tableName, array $columns, ?string $schema = null): void
    {
        // Generate columns statement
        $columnsSql = [];
        foreach ($columns as $name => $sqlDef) {
            $columnsSql[] = $this->quoteIdentifier($name) . ' ' . $sqlDef;
        }

        $tablePath = [];
        if ($schema) {
            $tablePath[] = $this->quoteIdentifier($schema);
        }
        $tablePath[] = $this->quoteIdentifier($tableName);

        // Create table
        $this->connection->query(sprintf(
            'CREATE TABLE IF NOT EXISTS %s (%s)',
            implode('.', $tablePath),
            implode(', ', $columnsSql),
        ));
    }
}
