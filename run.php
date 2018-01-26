<?php
use Keboola\DbExtractor\SnowflakeApplication;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Logger;
use Monolog\Handler\NullHandler;
use Symfony\Component\Yaml\Yaml;

define('APP_NAME', 'ex-db-snowflake');
define('ROOT_PATH', __DIR__);

require_once(dirname(__FILE__) . "/vendor/keboola/db-extractor-common/bootstrap.php");

$logger = new \Keboola\DbExtractor\Logger(APP_NAME);

try {
    $runAction = true;

    $arguments = getopt("d::", ["data::"]);
    if (!isset($arguments["data"])) {
        throw new UserException('Data folder not set.');
    }

    $app = new SnowflakeApplication(
        Yaml::parse(
            file_get_contents($arguments["data"] . "/config.yml")
        ),
        $arguments["data"]
    );

    if ($app['action'] !== 'run') {
        $app['logger']->setHandlers(array(new NullHandler(Logger::INFO)));
        $runAction = false;
    }

    $result = $app->run();

    if (!$runAction) {
        echo json_encode($result);
    }

    $app['logger']->log('info', "Extractor finished successfully.");
    exit(0);
} catch (UserException $e) {
    $logger->log('error', $e->getMessage(), (array) $e->getData());

    if (!$runAction) {
        echo $e->getMessage();
    }

    exit(1);
} catch (ApplicationException $e) {
    $logger->log('error', $e->getMessage(), (array) $e->getData());
    exit(2);
} catch (\Exception $e) {
    print $e->getMessage();
    print $e->getTraceAsString();

    exit(2);
}
