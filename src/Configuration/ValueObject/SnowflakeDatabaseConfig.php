<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration\ValueObject;

use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\SSLConnectionConfig;
use Keboola\DbExtractorConfig\Exception\PropertyNotSetException;

class SnowflakeDatabaseConfig extends DatabaseConfig
{
    private ?string $warehouse;

    private ?string $roleName;
    private ?string $logLevel;

    public static function fromArray(array $data): DatabaseConfig
    {
        $sslEnabled = !empty($data['ssl']) && !empty($data['ssl']['enabled']);

        return new self(
            $data['host'],
            $data['port'] ? (string) $data['port'] : null,
            $data['user'],
            $data['#password'],
            $data['database'] ?? null,
            $data['schema'] ?? null,
            $data['warehouse'] ?? null,
            $data['roleName'] ?? null,
            $sslEnabled ? SSLConnectionConfig::fromArray($data['ssl']) : null,
            $data['logLevel'] ?? null,
        );
    }

    public function __construct(
        string $host,
        ?string $port,
        string $username,
        string $password,
        ?string $database,
        ?string $schema,
        ?string $warehouse,
        ?string $roleName,
        ?SSLConnectionConfig $sslConnectionConfig,
        ?string $logLevel,
    ) {
        $this->warehouse = $warehouse;
        $this->roleName = $roleName;
        $this->logLevel = $logLevel;

        parent::__construct($host, $port, $username, $password, $database, $schema, $sslConnectionConfig, []);
    }

    public function hasWarehouse(): bool
    {
        return $this->warehouse !== null;
    }

    public function getWarehouse(): string
    {
        if ($this->warehouse === null) {
            throw new PropertyNotSetException('Property "warehouse" is not set.');
        }
        return $this->warehouse;
    }

    public function hasRoleName(): bool
    {
        return $this->roleName !== null;
    }

    public function getRoleName(): string
    {
        if ($this->roleName === null) {
            throw new PropertyNotSetException('Property "roleName" is not set.');
        }

        return $this->roleName;
    }

    public function getPassword(bool $escapeSemicolon = false): string
    {
        if ($escapeSemicolon && is_int(strpos(parent::getPassword(), ';'))) {
            return '{' . str_replace('}', '}}', parent::getPassword()) . '}';
        }
        return parent::getPassword();
    }

    public function getLogLevel(): ?string
    {
        return $this->logLevel;
    }
}
