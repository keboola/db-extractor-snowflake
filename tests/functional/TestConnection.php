<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\FunctionalTests;

use Keboola\SnowflakeDbAdapter\Connection;

class TestConnection
{
    public static function getDbConfigArray(): array
    {
        return [
            'host' => (string) getenv('SNOWFLAKE_DB_HOST'),
            'port' => (string) getenv('SNOWFLAKE_DB_PORT'),
            'user' => (string) getenv('SNOWFLAKE_DB_USER'),
            'keyPair' => (string) getenv('SNOWFLAKE_DB_PRIVATE_KEY'),
            'password' => (string) getenv('SNOWFLAKE_DB_PASSWORD'),
            'database' => (string) getenv('SNOWFLAKE_DB_DATABASE'),
            'schema' => (string) getenv('SNOWFLAKE_DB_SCHEMA'),
            'warehouse' => (string) getenv('SNOWFLAKE_DB_WAREHOUSE'),
        ];
    }

    public static function createConnection(): Connection
    {
        return new Connection(self::getDbConfigArray());
    }
}
