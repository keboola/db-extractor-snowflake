<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TraitTests\Tables;

use Keboola\DbExtractor\TraitTests\CreateTableTrait;
use Keboola\DbExtractor\TraitTests\InsertRowsTrait;
use Keboola\DbExtractor\TraitTests\QuoteIdentifierTrait;
use Keboola\DbExtractor\TraitTests\QuoteTrait;

/**
 * Dedicated fixture: commenting the shared tables would change the manifests of every other functional test.
 */
trait CommentsTableTrait
{
    use CreateTableTrait;
    use InsertRowsTrait;
    use QuoteIdentifierTrait;
    use QuoteTrait;

    public function createCommentsTable(string $name = 'comments'): void
    {
        $this->createTable($name, $this->getCommentsColumns());

        $this->connection->query(sprintf(
            'COMMENT ON TABLE %s IS %s',
            $this->quoteIdentifier($name),
            $this->quote('Table level comment'),
        ));
    }

    public function createCommentsView(string $name = 'comments_view', string $table = 'comments'): void
    {
        // Unlike MySQL, Snowflake supports comments on a view and on its columns
        $this->connection->query(sprintf(
            'CREATE VIEW %s (%s COMMENT %s, %s) COMMENT = %s AS SELECT %s, %s FROM %s',
            $this->quoteIdentifier($name),
            $this->quoteIdentifier('id'),
            $this->quote('View column comment'),
            $this->quoteIdentifier('name'),
            $this->quote('View level comment'),
            $this->quoteIdentifier('id'),
            $this->quoteIdentifier('name'),
            $this->quoteIdentifier($table),
        ));
    }

    public function generateCommentsRows(string $tableName = 'comments'): void
    {
        $data = $this->getCommentsRows();
        $this->insertRows($tableName, $data['columns'], $data['data']);
    }

    private function getCommentsColumns(): array
    {
        return [
            'id' => 'INT NOT NULL COMMENT \'Surrogate key\'',
            'name' => 'VARCHAR(100) COMMENT \'Customer name\'',
            'note' => 'VARCHAR(100)',
            // An empty COMMENT must be reported as no description, not as an empty one
            'empty_comment' => 'VARCHAR(100) COMMENT \'\'',
        ];
    }

    private function getCommentsRows(): array
    {
        return [
            'columns' => ['id', 'name', 'note', 'empty_comment'],
            'data' => [
                [1, 'Alice', 'first', 'x'],
                [2, 'Bob', 'second', 'y'],
            ],
        ];
    }
}
