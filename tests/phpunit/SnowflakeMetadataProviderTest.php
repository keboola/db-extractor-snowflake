<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Configuration\ValueObject\SnowflakeDatabaseConfig;
use Keboola\DbExtractor\Extractor\SnowflakeConnectionFactory;
use Keboola\DbExtractor\Extractor\SnowflakeMetadataProvider;
use Keboola\DbExtractor\FunctionalTests\TestConnection;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\Table;
use Keboola\DbExtractor\TraitTests\RemoveAllTablesTrait;
use Keboola\DbExtractor\TraitTests\Tables\CommentsTableTrait;
use Keboola\SnowflakeDbAdapter\Connection;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class SnowflakeMetadataProviderTest extends TestCase
{
    use CommentsTableTrait;
    use RemoveAllTablesTrait;

    protected Connection $connection;

    protected function setUp(): void
    {
        parent::setUp();
        $this->connection = TestConnection::createConnection();
        $this->removeAllTables();

        $this->createCommentsTable();
        $this->generateCommentsRows();
        $this->createCommentsView();
    }

    public function testCommentsArePropagatedAsDescriptions(): void
    {
        $table = $this->getTableMetadata('comments');

        $this->assertTrue($table->hasDescription());
        $this->assertSame('Table level comment', $table->getDescription());

        $columns = $table->getColumns();
        $this->assertSame('Surrogate key', $columns->getByName('id')->getDescription());
        $this->assertSame('Customer name', $columns->getByName('name')->getDescription());
        $this->assertFalse($columns->getByName('note')->hasDescription());
        $this->assertFalse($columns->getByName('empty_comment')->hasDescription());
    }

    public function testViewCommentsArePropagatedAsDescriptions(): void
    {
        $view = $this->getTableMetadata('comments_view');

        $this->assertSame('View level comment', $view->getDescription());
        $this->assertSame('View column comment', $view->getColumns()->getByName('id')->getDescription());
    }

    public function testDisabledPropagationReadsNoDescriptions(): void
    {
        foreach (['comments', 'comments_view'] as $tableName) {
            $table = $this->getTableMetadata($tableName, false);

            $this->assertFalse($table->hasDescription());
            foreach ($table->getColumns() as $column) {
                $this->assertFalse($column->hasDescription());
            }
        }
    }

    private function getTableMetadata(string $tableName, bool $propagateDescriptions = true): Table
    {
        $dbConfig = TestConnection::getDbConfigArray();
        // Key-pair auth, the test user requires MFA for password logins
        $dbConfig['#privateKey'] = $dbConfig['privateKey'];
        unset($dbConfig['password'], $dbConfig['privateKey']);
        $databaseConfig = SnowflakeDatabaseConfig::fromArray($dbConfig);

        $provider = new SnowflakeMetadataProvider(
            (new SnowflakeConnectionFactory(new NullLogger(), 1))->create($databaseConfig),
            $databaseConfig,
            $propagateDescriptions,
        );

        return $provider->listTables()->getByNameAndSchema($tableName, $databaseConfig->getSchema());
    }
}
