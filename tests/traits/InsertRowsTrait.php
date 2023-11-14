<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TraitTests;

use Keboola\DbExtractor\Exception\UserException;
use Keboola\SnowflakeDbAdapter\Connection;
use Throwable;

trait InsertRowsTrait
{
    use QuoteTrait;
    use QuoteIdentifierTrait;

    protected Connection $connection;

    public function insertRows(string $tableName, array $columns, array $rows): void
    {
        // Generate columns statement
        $columnsSql = [];
        foreach ($columns as $name) {
            $columnsSql[] = $this->quoteIdentifier($name);
        }

        // Generate values statement
        $valuesSql = [];
        foreach ($rows as $row) {
            $valuesSql[] =
                '(' .
                implode(
                    ', ',
                    array_map(function ($value) {
                        if ($value === null) {
                            return 'NULL';
                        }
                        return $this->quote((string) $value);
                    }, $row),
                ) .
                ')';
        }
        // In informix cannot be multiple values in one INSERT statement
        try {
            $this->connection->query(sprintf(
                'INSERT INTO %s (%s) VALUES %s',
                $this->quoteIdentifier($tableName),
                implode(', ', $columnsSql),
                implode(', ', $valuesSql),
            ));
        } catch (Throwable $e) {
            throw new UserException($e->getMessage(), $e->getCode(), $e);
        }
    }
}
