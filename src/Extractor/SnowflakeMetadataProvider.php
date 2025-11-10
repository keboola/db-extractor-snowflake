<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Datatype\Definition\Snowflake as SnowflakeDatatype;
use Keboola\DbExtractor\Adapter\Metadata\MetadataProvider;
use Keboola\DbExtractor\Adapter\ODBC\OdbcConnection;
use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\ColumnBuilder;
use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\MetadataBuilder;
use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\TableBuilder;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\ColumnCollection;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\Table;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\TableCollection;
use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\InputTable;

class SnowflakeMetadataProvider implements MetadataProvider
{
    private OdbcConnection $db;

    private DatabaseConfig $databaseConfig;

    public function __construct(OdbcConnection $db, DatabaseConfig $databaseConfig)
    {
        $this->db = $db;
        $this->databaseConfig = $databaseConfig; // database is optional
    }

    public function getTable(InputTable $table): Table
    {
        return $this
            ->listTables([$table])
            ->getByNameAndSchema($table->getName(), $table->getSchema());
    }

    /**
     * @param array|InputTable[] $whitelist
     * @param bool $loadColumns if false, columns metadata are NOT loaded, useful useful if there are a lot of tables
     */
    public function listTables(array $whitelist = [], bool $loadColumns = true): TableCollection
    {
        $tableBuilders = [];

        // Process tables
        $tableRequiredProperties = ['schema', 'type', 'rowCount'];
        $columnRequiredProperties= ['ordinalPosition', 'nullable'];

        $builder = MetadataBuilder::create($tableRequiredProperties, $columnRequiredProperties);
        $sqlWhereElements = [];
        foreach ($this->queryTables($whitelist) as $item) {
            $tableId = $item['schema_name'] . '.' . $item['name'];
            $tableBuilder = $builder->addTable();
            $tableBuilders[$tableId] = $tableBuilder;

            if ($loadColumns === false) {
                $tableBuilder->setColumnsNotExpected();
            }

            $this->processTableData($tableBuilder, $item);
            $sqlWhereElements[] = sprintf(
                '(table_schema = %s AND table_name = %s)',
                $this->db->quote($item['schema_name']),
                $this->db->quote($item['name']),
            );
        }

        if ($sqlWhereElements && $loadColumns) {
            foreach ($this->queryColumns($sqlWhereElements) as $column) {
                $tableId = $column['TABLE_SCHEMA'] . '.' . $column['TABLE_NAME'];
                if (!isset($tableBuilders[$tableId])) {
                    continue;
                }
                $columnBuilder = $tableBuilders[$tableId]->addColumn();
                $this->processColumnData(
                    $columnBuilder,
                    $column,
                    $this->queryPrimaryKeys($column['TABLE_SCHEMA'], $column['TABLE_NAME']),
                );
            }
        }

        return $builder->build();
    }

    public function getColumnsInfo(string $query): ColumnCollection
    {
        // Create temporary view from the supplied query
        $sql = sprintf(
            'SELECT * FROM (%s) LIMIT 0;',
            rtrim(trim($query), ';'),
        );
        $this->db->query($sql)->fetchAll();
        $columnsRaw = $this->db->query('DESC RESULT LAST_QUERY_ID()')->fetchAll();

        $columns = [];
        foreach ($columnsRaw as $data) {
            [$type, $length] = SnowflakeUtils::parseTypeAndLength($data['type']);

            // to solve Snowflake VARCHAR(0) issue for null columns
            // https://community.snowflake.com/s/case/500VI00000DZKyuYAH/inconsistency-in-describe-result-for-null-columns
            if ($type === 'VARCHAR' && ($length === '0' || $length > SnowflakeDatatype::MAX_VARCHAR_LENGTH)) {
                $length = (string) SnowflakeDatatype::MAX_VARCHAR_LENGTH;
            }

            $builder = ColumnBuilder::create();
            $builder->setName($data['name']);
            $builder->setType($type);
            $builder->setLength($length);
            $columns[] = $builder->build();
        }

        return new ColumnCollection($columns);
    }

    private function processTableData(TableBuilder $builder, array $data): void
    {
        $isView = array_key_exists('text', $data);

        $builder
            ->setSchema($data['schema_name'])
            ->setName($data['name'])
            ->setCatalog($data['database_name'] ?? null)
            ->setType($isView ? 'VIEW' : $data['kind'])
            ->setRowCount(isset($data['rows']) ? (int) $data['rows'] : 0)
            ->setDatatypeBackend(SnowflakeDatatype::METADATA_BACKEND)
        ;
    }

    private function queryColumns(array $queryTables): array
    {
        $sqlWhereClause = sprintf('WHERE %s', implode(' OR ', $queryTables));

        $sql = sprintf(
            'SELECT * FROM information_schema.columns %s ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION',
            $sqlWhereClause,
        );

        return $this->db->query($sql)->fetchAll();
    }

    private function queryTables(?array $whiteList): array
    {
        $sql = $this->databaseConfig->hasSchema() ? 'SHOW TABLES IN SCHEMA' : 'SHOW TABLES IN DATABASE';
        $tables = $this->db->query($sql)->fetchAll();

        $sql = $this->databaseConfig->hasSchema() ? 'SHOW VIEWS IN SCHEMA' : 'SHOW VIEWS IN DATABASE';
        $views = $this->db->query($sql)->fetchAll();

        $result = array_merge($tables, $views);
        $filteredResult = array_filter($result, function ($v) use ($whiteList) {
            return !$this->shouldTableBeSkipped($v, $whiteList);
        });

        usort($filteredResult, function ($item1, $item2) {
            return strnatcmp($item1['name'], $item2['name']);
        });

        return $filteredResult;
    }

    /**
     * @param null|InputTable[] $whiteList
     */
    private function shouldTableBeSkipped(array $table, ?array $whiteList): bool
    {
        $isFromDifferentSchema = $this->databaseConfig->hasSchema() &&
            $table['schema_name'] !== $this->databaseConfig->getSchema();

        $isFromInformationSchema = $table['schema_name'] === 'INFORMATION_SCHEMA';
        $isNotFromWhiteList = false;
        if ($whiteList) {
            $filteredWhiteList = array_filter($whiteList, function (InputTable $v) use ($table) {
                return $v->getSchema() === $table['schema_name'] && $v->getName() === $table['name'];
            });
            $isNotFromWhiteList = empty($filteredWhiteList);
        }
        return $isFromDifferentSchema || $isFromInformationSchema || $isNotFromWhiteList;
    }

    private function queryPrimaryKeys(string $schema, string $tableName): array
    {
        $sql = sprintf(
            'SHOW PRIMARY KEYS IN TABLE %s.%s',
            $this->db->quoteIdentifier($schema),
            $this->db->quoteIdentifier($tableName),
        );
        $primaryKeyColumns = $this->db->query($sql)->fetchAll();

        return array_map(function ($row) {
            return $row['column_name'];
        }, $primaryKeyColumns);
    }

    private function processColumnData(ColumnBuilder $columnBuilder, array $column, array $primaryKeys): void
    {
        $length = ($column['CHARACTER_MAXIMUM_LENGTH']) ? $column['CHARACTER_MAXIMUM_LENGTH'] : null;
        if (is_null($length) && !is_null($column['NUMERIC_PRECISION'])) {
            if (is_numeric($column['NUMERIC_SCALE'])) {
                $length = $column['NUMERIC_PRECISION'] . ',' . $column['NUMERIC_SCALE'];
            } else {
                $length = $column['NUMERIC_PRECISION'];
            }
        }
        $columnBuilder
            ->setName($column['COLUMN_NAME'])
            ->setDefault($column['COLUMN_DEFAULT'])
            ->setLength($length)
            ->setNullable(!(trim($column['IS_NULLABLE']) === 'NO'))
            ->setType($column['DATA_TYPE'])
            ->setOrdinalPosition((int) $column['ORDINAL_POSITION']);

        if (in_array($column['COLUMN_NAME'], $primaryKeys, true)) {
            $columnBuilder->setPrimaryKey(true);
        }
    }
}
