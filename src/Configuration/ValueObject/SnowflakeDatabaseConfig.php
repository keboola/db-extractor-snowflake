<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration\ValueObject;

use \Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\SSLConnectionConfig;
use Keboola\DbExtractorConfig\Exception\PropertyNotSetException;

class SnowflakeDatabaseConfig extends DatabaseConfig
{
    private ?string $warehouse;
    private string $fileFormat;

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
            $sslEnabled ? SSLConnectionConfig::fromArray($data['ssl']) : null,
            $data['exportFileFormat']
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
        ?SSLConnectionConfig $sslConnectionConfig,
        string $fileFormat
    ) {
        $this->warehouse = $warehouse;
        $this->fileFormat = $fileFormat;

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

    public function getPassword(bool $escapeSemicolon = false): string
    {
        if ($escapeSemicolon && is_int(strpos(parent::getPassword(), ';'))) {
            return '{' . str_replace('}', '}}', parent::getPassword()) . '}';
        }
        return parent::getPassword();
    }

    public function getFileFormat(): string
    {
        return $this->fileFormat;
    }
}
