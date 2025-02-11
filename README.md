# Snowflake DB Extractor
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/keboola/db-extractor-snowflake/blob/master/LICENSE.md)

Docker application for exporting data from Snowflake Data Warehouse 

![extraction flow](https://github.com/keboola/db-extractor-snowflake/blob/master/docs/snowflake-ex-flow.png)


## Configuration

    {
      "parameters": {
        "db": {
          "host": "HOST",
          "port": "PORT",
          "database": "DATABASE",
          "database": "SCHEMA",
          "warehouse": "WAREHOUSE",
          "user": "USERNAME",
          "#password": "PASSWORD"
        },
        "tables": [
          {
            "name": "employees",
            "query": "SELECT * FROM employees",
            "outputTable": "in.c-main.employees",
            "incremental": false,
            "enabled": true,
            "primaryKey": null
          }
        ]
      }
    }

## Snowflake Privileges Templates

### Development

Required snowflake resource for extractor:

```
CREATE DATABASE "snowflake_extractor";
USE DATABASE "snowflake_extractor";
CREATE SCHEMA "snowflake_extractor";
CREATE WAREHOUSE "snowflake_extractor" WITH 
  WAREHOUSE_SIZE = 'XSMALL' 
  WAREHOUSE_TYPE = 'STANDARD' 
  AUTO_SUSPEND = 900 
  AUTO_RESUME = TRUE;
CREATE ROLE "snowflake_extractor";
GRANT USAGE ON WAREHOUSE "snowflake_extractor" TO ROLE "snowflake_extractor";
GRANT USAGE ON DATABASE "snowflake_extractor" TO ROLE "snowflake_extractor";
GRANT USAGE ON SCHEMA "snowflake_extractor" TO ROLE "snowflake_extractor";
GRANT SELECT ON ALL TABLES IN SCHEMA "snowflake_extractor" TO ROLE "snowflake_extractor";
GRANT SELECT ON ALL VIEWS IN SCHEMA "snowflake_extractor" TO ROLE "snowflake_extractor";
CREATE USER "snowflake_extractor" 
  PASSWORD = 'password' 
  DEFAULT_ROLE = "snowflake_extractor" 
  DEFAULT_WAREHOUSE = "snowflake_extractor" 
  DEFAULT_NAMESPACE = "snowflake_extractor"."snowflake_extractor" 
  MUST_CHANGE_PASSWORD = FALSE;
GRANT ROLE "snowflake_extractor" TO USER "snowflake_extractor";
```

Note that `GRANT SELECT ON ALL *` queries will grant permissions to objects existing at the execution time only. New objects will need to be granted to the role as they are created.  

## Running Tests

1. Download Snowflake drivers
 - snowflake-odbc-x86_64.deb
 - snowsql-linux_x86_64.bash
2. Create snowflake resources (database, schema, role and user) [note that the test user/role must have permissions on the Public schema of the test database]
3. Additional snowflake resource for extractor tests:
```
GRANT ALL ON DATABASE "snowflake_extractor" TO ROLE "snowflake_extractor";
GRANT ALL ON SCHEMA "snowflake_extractor" TO ROLE "snowflake_extractor";
GRANT ALL ON SCHEMA "PUBLIC" TO ROLE "snowflake_extractor";
GRANT ALL ON ALL TABLES IN SCHEMA "snowflake_extractor" TO ROLE "snowflake_extractor";
GRANT ALL ON ALL VIEWS IN SCHEMA "snowflake_extractor" TO ROLE "snowflake_extractor";
```

4. Create `.env` file and fill in you Snowflake credentials:
```
SNOWFLAKE_DB_HOST=
SNOWFLAKE_DB_PORT=443
SNOWFLAKE_DB_USER=
SNOWFLAKE_DB_PASSWORD=
SNOWFLAKE_DB_DATABASE=
SNOWFLAKE_DB_SCHEMA=
SNOWFLAKE_DB_WAREHOUSE=
SNOWFLAKE_DB_ROLE_NAME=
KBC_RUNID=123456
```
5. Install composer dependencies locally and load test fixtures to S3
```$xslt
docker-compose run --rm dev composer install
```
6. Run the tests:

```
docker-compose run --rm app composer ci
```

Run single test example:
```
docker-compose run --rm dev ./vendor/bin/phpunit --debug --filter testGetTables
```

## License

MIT licensed, see [LICENSE](./LICENSE) file.
