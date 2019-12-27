<?php

declare(strict_types=1);

use Keboola\DbExtractor\SnowflakeApplication;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractorLogger\Logger;
use Monolog\Handler\NullHandler;
use Symfony\Component\Serializer\Encoder\JsonEncode;
use Symfony\Component\Serializer\Encoder\JsonEncoder;
use Symfony\Component\Serializer\Encoder\JsonDecode;

require_once __DIR__ . '/vendor/autoload.php';

$logger = new Logger('ex-db-snowflake');

try {
    $runAction = true;

    $arguments = getopt('d::', ['data::']);
    if (!isset($arguments['data'])) {
        throw new UserException('Data folder not set.');
    }

    if (file_exists($arguments['data'] . '/config.json')) {
        $config = json_decode(
            file_get_contents($arguments['data'] . '/config.json'),
            true
        );
    } else {
        throw new UserException('Could not find configuration file');
    }

    // get the state
    $inputState = [];
    $inputStateFile = $arguments['data'] . '/in/state.json';
    if (file_exists($inputStateFile)) {
        $jsonDecode = new JsonDecode(true);
        $inputState = $jsonDecode->decode(
            file_get_contents($inputStateFile),
            JsonEncoder::FORMAT
        );
    }

    $app = new SnowflakeApplication($config, $logger, $inputState, $arguments['data']);

    if ($app['action'] !== 'run') {
        $app['logger']->setHandlers(array(new NullHandler(Logger::INFO)));
        $runAction = false;
    }

    $result = $app->run();

    if (!$runAction) {
        echo json_encode($result);
    } else {
        if (!empty($result['state'])) {
            // write state
            $outputStateFile = $arguments['data'] . '/out/state.json';
            $jsonEncode = new JsonEncode();
            file_put_contents($outputStateFile, $jsonEncode->encode($result['state'], JsonEncoder::FORMAT));
        }
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
