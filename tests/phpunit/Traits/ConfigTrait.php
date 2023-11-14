<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests\Traits;

use Keboola\DbExtractor\FunctionalTests\TestConnection;

trait ConfigTrait
{
    private function getConfig(): array
    {
        $configTemplate = <<<JSON
{
  "parameters": {
    "db": %s,
    "data_dir": "SNOWFLAKE",
    "tables": [
      {
        "id": 1,
        "name": "sales",
        "query": "SELECT * FROM \"sales\" WHERE \"categorygroup\" NOT LIKE '%%stupidsprintf'",
        "outputTable": "in.c-main.sales",
        "incremental": false,
        "primaryKey": null,
        "enabled": true
      },
      {
        "id": 2,
        "name": "escaping",
        "query": "SELECT * FROM \"escaping\";",
        "outputTable": "in.c-main.escaping",
        "incremental": false,
        "primaryKey": [
          "col1"
        ],
        "enabled": true
      },
      {
        "id": 3,
        "name": "tableColumns",
        "outputTable": "in.c-main.tableColumns",
        "incremental": false,
        "primaryKey": null,
        "enabled": true,
        "table": {
          "schema": "%s",
          "tableName": "types"
        },
        "columns": [
          "character",
          "integer",
          "decimal",
          "date"
        ]
      }
    ]
  }
}
JSON;
        /** @var array<array> $config */
        $config = (array) json_decode(
            sprintf(
                $configTemplate,
                json_encode(TestConnection::getDbConfigArray()),
                (string) getenv('SNOWFLAKE_DB_SCHEMA'),
            ),
            true,
        );
        $config['parameters']['db']['#password'] = $config['parameters']['db']['password'];
        unset($config['parameters']['db']['password']);
        return $config;
    }

    public function getIncrementalConfig(): array
    {
        $configTemplate = <<<JSON
{
  "parameters": {
    "db": %s,
    "data_dir": "SNOWFLAKE",
    "name": "auto-increment-timestamp",
    "outputTable": "in.c-main.auto-increment-timestamp",
    "incremental": true,
    "primaryKey": ["id"],
    "incrementalFetchingColumn": "id",
    "table": {
        "tableName": "auto Increment Timestamp",
        "schema": "%s"    
    }
  }
}
JSON;
        /** @var array<array> $config */
        $config = (array) json_decode(
            sprintf(
                $configTemplate,
                json_encode(TestConnection::getDbConfigArray()),
                (string) getenv('SNOWFLAKE_DB_SCHEMA'),
            ),
            true,
        );
        $config['parameters']['db']['#password'] = $config['parameters']['db']['password'];
        unset($config['parameters']['db']['password']);
        return $config;
    }
}
