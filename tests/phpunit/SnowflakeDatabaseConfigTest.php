<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Generator;
use Keboola\DbExtractor\Configuration\ValueObject\SnowflakeDatabaseConfig;
use Keboola\DbExtractor\Tests\Traits\ConfigTrait;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;

class SnowflakeDatabaseConfigTest extends TestCase
{
    use ConfigTrait;

    /**
     * @dataProvider passwordsDataProvider
     */
    public function testPasswords(string $password, string $expectedPassword): void
    {
        $config = $this->getConfig();
        $config['parameters']['db']['#password'] = $password;
        /** @var SnowflakeDatabaseConfig $databaseConfig */
        $databaseConfig = SnowflakeDatabaseConfig::fromArray($config['parameters']['db']);

        Assert::assertEquals($expectedPassword, $databaseConfig->getPassword(true));
    }

    public function passwordsDataProvider(): Generator
    {
        yield 'simple-password' => [
            'AbcdEfgh123456',
            'AbcdEfgh123456',
        ];

        yield 'password-with-semicolon' => [
            'AbcdEfgh12;3456',
            '{AbcdEfgh12;3456}',
        ];

        yield 'password-with-bracket' => [
            'AbcdEfgh12}3456',
            'AbcdEfgh12}3456',
        ];

        yield 'password-with-semicolon-and-bracket' => [
            'AbcdEf;gh12}3456',
            '{AbcdEf;gh12}}3456}',
        ];

        yield 'password-starts-with-semicolon' => [
            ';AbcdEfgh12345}6',
            '{;AbcdEfgh12345}}6}',
        ];
    }

    public function testLogLevel(): void
    {
        $config = $this->getConfig();
        $config['parameters']['db']['logLevel'] = 'DEBUG';
        /** @var SnowflakeDatabaseConfig $databaseConfig */
        $databaseConfig = SnowflakeDatabaseConfig::fromArray($config['parameters']['db']);

        Assert::assertEquals('DEBUG', $databaseConfig->getLogLevel());
    }
}
