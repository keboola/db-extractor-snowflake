<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration\ValueObject;

use Keboola\DbExtractor\Configuration\Exception\PrivateKeyIsNotValid;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\SSLConnectionConfig;
use Keboola\DbExtractorConfig\Exception\PropertyNotSetException;

class SnowflakeDatabaseConfig extends DatabaseConfig
{
    private ?string $warehouse;

    private ?string $roleName;

    private ?string $privateKey;

    public static function fromArray(array $data): DatabaseConfig
    {
        $sslEnabled = !empty($data['ssl']) && !empty($data['ssl']['enabled']);

        return new self(
            $data['host'],
            $data['port'] ? (string) $data['port'] : null,
            $data['user'],
            $data['#password'] ?? '',
            $data['database'] ?? null,
            $data['schema'] ?? null,
            $data['warehouse'] ?? null,
            $data['roleName'] ?? null,
            $sslEnabled ? SSLConnectionConfig::fromArray($data['ssl']) : null,
            $data['#privateKey'] ?? null,
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
        ?string $privateKey,
    ) {
        if (empty($password) && $privateKey === null) {
            throw new PropertyNotSetException('Either "password" or "privateKey" must be provided.');
        }

        if (!empty($password) && !empty($privateKey)) {
            throw new UserException('Both "password" and "privateKey" cannot be set at the same time.');
        }

        $this->warehouse = $warehouse;
        $this->roleName = $roleName;
        $this->privateKey = $privateKey;

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

    public function hasPrivateKey(): bool
    {
        return $this->privateKey !== null;
    }

    public function getPrivateKey(): string
    {
        if ($this->privateKey === null || $this->privateKey === '') {
            throw new PropertyNotSetException('Property "privateKey" is not set.');
        }

        return $this->privateKey;
    }

    public function getPrivateKeyPath(): string
    {
        $privateKeyResource = openssl_pkey_get_private($this->getPrivateKey());
        if (!$privateKeyResource) {
            throw new PrivateKeyIsNotValid();
        }

        $pemPKCS8 = '';
        openssl_pkey_export($privateKeyResource, $pemPKCS8);

        $privateKeyPath = tempnam(sys_get_temp_dir(), 'snowflake_private_key_' . uniqid()) . '.p8';
        file_put_contents($privateKeyPath, $pemPKCS8);

        return $privateKeyPath;
    }
}
