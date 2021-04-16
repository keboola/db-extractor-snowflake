<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Generator;
use Keboola\DbExtractor\Adapter\Connection\DbConnection;
use Keboola\DbExtractor\Adapter\Query\DefaultQueryFactory;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;

class SnowflakeQueryFactory extends DefaultQueryFactory
{
    public const SEMI_STRUCTURED_TYPES = ['VARIANT' , 'OBJECT', 'ARRAY'];

    private SnowflakeOdbcConnection $connection;

    private SnowflakeMetadataProvider $metadataProvider;

    private ?string $incrementalFetchingColType = null;

    public function __construct(
        SnowflakeOdbcConnection $connection,
        SnowflakeMetadataProvider $metadataProvider,
        array $state
    ) {
        $this->connection = $connection;
        $this->metadataProvider = $metadataProvider;
        parent::__construct($state);
    }

    public function setIncrementalFetchingColType(string $incrementalFetchingColType): self
    {
        $this->incrementalFetchingColType = $incrementalFetchingColType;
        return $this;
    }

    public function create(ExportConfig $exportConfig, DbConnection $connection): string
    {
        $query = parent::create($exportConfig, $this->connection);
        $columnInfo = $this->metadataProvider->getColumnInfo($query);
        $objectColumns = array_filter($columnInfo, function ($column): bool {
            return in_array($column['type'], self::SEMI_STRUCTURED_TYPES);
        });

        if (empty($objectColumns)) {
            return $query;
        }

        return $this->createWithCasting($exportConfig, $this->connection, $columnInfo);
    }

    public function createIncrementalAddon(ExportConfig $exportConfig, DbConnection $connection): string
    {
        $sql = array_merge(
            iterator_to_array($this->createWhere($exportConfig, $connection)),
            iterator_to_array($this->createOrderBy($exportConfig, $connection))
        );
        return implode(' ', $sql);
    }

    protected function createWithCasting(
        ExportConfig $exportConfig,
        DbConnection $connection,
        array $columnInfo
    ): string {
        $sql = array_merge(
            iterator_to_array($this->createSelectWithCast($connection, $columnInfo)),
            iterator_to_array($this->createFrom($exportConfig, $connection))
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
            if (in_array($column['type'], self::SEMI_STRUCTURED_TYPES)) {
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
