docker pull quay.io/keboola/developer-portal-cli-v2:latest
export REPOSITORY=`docker run --rm -e KBC_DEVELOPERPORTAL_USERNAME=$KBC_DEVELOPERPORTAL_USERNAME -e KBC_DEVELOPERPORTAL_PASSWORD=$KBC_DEVELOPERPORTAL_PASSWORD -e KBC_DEVELOPERPORTAL_URL=$KBC_DEVELOPERPORTAL_URL quay.io/keboola/developer-portal-cli-v2:latest ecr:get-repository keboola keboola.ex-db-snowflake`
docker tag keboola/db-extractor-snowflake:latest $REPOSITORY:$TRAVIS_TAG
docker tag keboola/db-extractor-snowflake:latest $REPOSITORY:latest
eval $(docker run --rm -e KBC_DEVELOPERPORTAL_USERNAME=$KBC_DEVELOPERPORTAL_USERNAME -e KBC_DEVELOPERPORTAL_PASSWORD=$KBC_DEVELOPERPORTAL_PASSWORD -e KBC_DEVELOPERPORTAL_URL=$KBC_DEVELOPERPORTAL_URL quay.io/keboola/developer-portal-cli-v2:latest ecr:get-login keboola keboola.ex-db-snowflake)
docker push $REPOSITORY:$TRAVIS_TAG
docker push $REPOSITORY:latest
eval $(docker run --rm -e KBC_DEVELOPERPORTAL_USERNAME=$KBC_DEVELOPERPORTAL_USERNAME -e KBC_DEVELOPERPORTAL_PASSWORD=$KBC_DEVELOPERPORTAL_PASSWORD -e KBC_DEVELOPERPORTAL_URL=$KBC_DEVELOPERPORTAL_URL quay.io/keboola/developer-portal-cli-v2:latest update-app-repository keboola keboola.ex-db-snowflake $TRAVIS_TAG)

export SYRUP_CLI=quay.io/keboola/syrup-cli

docker pull $SYRUP_CLI:latest

echo 'Running simple configuration ... job[1/1]'
docker run --rm -e KBC_STORAGE_TOKEN=$KBC_SYRUP_CLI_TOKEN \
   $SYRUP_CLI:latest run-job keboola.ex-db-snowflake 288409174 $TRAVIS_TAG

if [ $? -ne 0 ]; then
  echo 'Simple test job run failed'
  exit 1;
fi

echo 'All test jobs were successfull.  Updating repository tag in the developer portal...'
docker run --rm \
  -e KBC_DEVELOPERPORTAL_USERNAME=$KBC_DEVELOPERPORTAL_USERNAME \
  -e KBC_DEVELOPERPORTAL_PASSWORD=$KBC_DEVELOPERPORTAL_PASSWORD \
  -e KBC_DEVELOPERPORTAL_URL=$KBC_DEVELOPERPORTAL_URL \
  quay.io/keboola/developer-portal-cli-v2:latest update-app-repository keboola keboola.ex-db-snowflake $TRAVIS_TAG
