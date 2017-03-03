#!/bin/bash
docker login -u="$QUAY_USERNAME" -p="$QUAY_PASSWORD" quay.io
docker tag keboola/db-extractor-snowflake quay.io/keboola/db-extractor-snowflake:$TRAVIS_TAG
docker tag keboola/db-extractor-snowflake quay.io/keboola/db-extractor-snowflake:latest
docker push quay.io/keboola/db-extractor-snowflake:$TRAVIS_TAG
docker push quay.io/keboola/db-extractor-snowflake:latest