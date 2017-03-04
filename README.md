# Snowflake DB Extractor
[![Build Status](https://travis-ci.org/keboola/db-extractor-snowflake.svg?branch=master)](https://travis-ci.org/keboola/db-extractor-snowflake)

    {
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
