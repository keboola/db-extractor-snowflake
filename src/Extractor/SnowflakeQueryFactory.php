<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Generator;
use Keboola\DbExtractor\Adapter\Connection\DbConnection;
use Keboola\DbExtractor\Adapter\Query\DefaultQueryFactory;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;

class SnowflakeQueryFactory extends DefaultQueryFactory
{
    private ?string $incrementalFetchingColType = null;

    public function setIncrementalFetchingColType(string $incrementalFetchingColType): self
    {
        $this->incrementalFetchingColType = $incrementalFetchingColType;
        return $this;
    }

    public function createWithCasting(ExportConfig $exportConfig, DbConnection $connection, array $columnInfo): string
    {
        $sql = array_merge(
            iterator_to_array($this->createSelectWithCast($connection, $columnInfo)),
            iterator_to_array($this->createFrom($exportConfig, $connection))
        );
        return implode(' ', $sql);
    }

    public function createIncrementalAddon(ExportConfig $exportConfig, DbConnection $connection): string
    {
        $sql = array_merge(
            iterator_to_array($this->createWhere($exportConfig, $connection)),
            iterator_to_array($this->createOrderBy($exportConfig, $connection))
        );
        return implode(' ', $sql);
    }

    protected function createWhere(ExportConfig $exportConfig, DbConnection $connection): Generator
    {
        if ($exportConfig->isIncrementalFetching() && isset($this->state['lastFetchedRow'])) {
            if ($this->incrementalFetchingColType === Snowflake::INCREMENT_TYPE_NUMERIC) {
                $lastFetchedRow = $this->state['lastFetchedRow'];
            } else {
                $lastFetchedRow = $connection->quote((string) $this->state['lastFetchedRow']);
            }
            yield sprintf(
            // intentionally ">=" last row should be included, it is handled by storage deduplication process
                'WHERE %s >= %s',
                $connection->quoteIdentifier($exportConfig->getIncrementalFetchingColumn()),
                $lastFetchedRow
            );
        }
    }

    protected function createSelectWithCast(DbConnection $connection, array $columnInfo): Generator
    {
        $columnCast = array_map(function ($column) use ($connection): string {
            if (in_array($column['type'], SnowsqlExportAdapter::SEMI_STRUCTURED_TYPES)) {
                return sprintf(
                    'CAST(%s AS TEXT) AS %s',
                    $connection->quoteIdentifier($column['name']),
                    $connection->quoteIdentifier($column['name'])
                );
            }
            return $connection->quoteIdentifier($column['name']);
        }, $columnInfo);

        yield sprintf('SELECT %s', implode(', ', $columnCast));
    }
}
