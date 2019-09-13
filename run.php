<?php

declare(strict_types=1);

use Keboola\DbExtractor\SnowflakeApplication;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractorLogger\Logger;
use Monolog\Handler\NullHandler;
use Symfony\Component\Yaml\Yaml;

require_once __DIR__ . '/vendor/autoload.php';

$logger = new Logger('ex-db-snowflake');

try {
    $runAction = true;

    $arguments = getopt('d::', ['data::']);
    if (!isset($arguments['data'])) {
        throw new UserException('Data folder not set.');
    }

    if (file_exists($arguments['data'] . '/config.yml')) {
        $config = Yaml::parse(
            file_get_contents($arguments['data'] . '/config.yml')
        );
    } else if (file_exists($arguments['data'] . '/config.json')) {
        $config = json_decode(
            file_get_contents($arguments['data'] . '/config.json'),
            true
        );
    } else {
        throw new UserException('Could not find configuration file');
    }

    $app = new SnowflakeApplication($config, $logger, [], $arguments['data']);

    if ($app['action'] !== 'run') {
        $app['logger']->setHandlers(array(new NullHandler(Logger::INFO)));
        $runAction = false;
    }

    $result = $app->run();

    if (!$runAction) {
        echo json_encode($result);
    }

    $app['logger']->log('info', 'Extractor finished successfully.');
    exit(0);
} catch (UserException $e) {
    $logger->log('error', $e->getMessage(), (array) $e->getData());
    if (!$runAction) {
        echo $e->getMessage();
    }
    exit(1);
} catch (\Throwable $e) {
    $logger->critical(
        get_class($e) . ':' . $e->getMessage(),
        [
            'errFile' => $e->getFile(),
            'errLine' => $e->getLine(),
            'errCode' => $e->getCode(),
            'errTrace' => $e->getTraceAsString(),
            'errPrevious' => $e->getPrevious() ? get_class($e->getPrevious()) : '',
        ]
    );
    exit(2);
}
