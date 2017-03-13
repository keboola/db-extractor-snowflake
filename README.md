# Snowflake DB Extractor
[![Build Status](https://travis-ci.org/keboola/db-extractor-snowflake.svg?branch=master)](https://travis-ci.org/keboola/db-extractor-snowflake) 
[![Code Climate](https://codeclimate.com/github/keboola/db-extractor-snowflake/badges/gpa.svg)](https://codeclimate.com/github/keboola/db-extractor-snowflake)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/keboola/db-extractor-snowflake/blob/master/LICENSE.md)

Docker application for exporting data from Snowflake Data Warehouse 

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
