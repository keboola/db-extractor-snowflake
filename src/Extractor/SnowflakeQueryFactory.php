<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Generator;
use Keboola\DbExtractor\Adapter\Connection\DbConnection;
use Keboola\DbExtractor\Adapter\Query\DefaultQueryFactory;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\Column;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;

class SnowflakeQueryFactory extends DefaultQueryFactory
{
    public const SEMI_STRUCTURED_TYPES = ['VARIANT' , 'OBJECT', 'ARRAY'];

    private SnowflakeMetadataProvider $metadataProvider;

    private ?string $incrementalFetchingColType = null;

    public function __construct(
        SnowflakeMetadataProvider $metadataProvider,
        array $state
    ) {
        $this->metadataProvider = $metadataProvider;
        parent::__construct($state);
    }

    public function setIncrementalFetchingColType(string $incrementalFetchingColType): self
    {
        $this->incrementalFetchingColType = $incrementalFetchingColType;
        return $this;
    }

    protected function createSelect(ExportConfig $exportConfig, DbConnection $connection): Generator
    {
        // Create query without casting to get columns metadata
        $rawQuery = implode(' ', array_merge(
            iterator_to_array(parent::createSelect($exportConfig, $connection)),
            iterator_to_array(parent::createFrom($exportConfig, $connection)),
        ));
        $columnsMetadata = $this->metadataProvider->getColumnsInfo($rawQuery);
        $structuredColumnsCount = count(array_filter($columnsMetadata->getAll(), function (Column $column) {
            return in_array($column->getType(), self::SEMI_STRUCTURED_TYPES);
        }));

        // No structured column -> use default implementation
        if ($structuredColumnsCount === 0) {
            yield from parent::createSelect($exportConfig, $connection);
            return;
        }

        // Cast semi-structured types to text
        $castedColumns = array_map(function (Column $column) use ($connection): string {
            if (in_array($column->getType(), self::SEMI_STRUCTURED_TYPES)) {
                return sprintf(
                    'CAST(%s AS TEXT) AS %s',
                    $connection->quoteIdentifier($column->getName()),
                    $connection->quoteIdentifier($column->getName())
                );
            }
            return $connection->quoteIdentifier($column->getName());
        }, $columnsMetadata->getAll());

        // Generate SELECT statement
        yield sprintf('SELECT %s', implode(', ', $castedColumns));
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
}
