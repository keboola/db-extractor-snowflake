<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Extractor\SnowflakeUtils;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;

class SnowflakeUtilsTest extends TestCase
{
    /**
     * @dataProvider getTestData
     */
    public function testParseTypeAndLength(string $rawType, array $expected): void
    {
        Assert::assertSame($expected, SnowflakeUtils::parseTypeAndLength($rawType));
    }

    public function getTestData(): iterable
    {
        yield [
            '',
            [null, null],
        ];

        yield [
            '(FOO)',
            [null, null],
        ];

        yield [
            'DATE',
            ['DATE', null],
        ];

        yield [
            'NUMBER(38,0)',
            ['NUMBER', '38,0'],
        ];

        yield [
            'VARCHAR(16777216)',
            ['VARCHAR', '16777216'],
        ];

        yield [
            'VARCHAR(16777216) COLLATE \'en-ps\'',
            ['VARCHAR', '16777216'],
        ];
    }

    /**
     * @dataProvider getColumnLengthData
     * @param array<string, mixed> $column
     */
    public function testGetColumnLength(array $column, ?string $expected): void
    {
        Assert::assertSame($expected, SnowflakeUtils::getColumnLength($column));
    }

    public function getColumnLengthData(): iterable
    {
        yield 'varchar uses character_maximum_length' => [
            $this->columnRow(['CHARACTER_MAXIMUM_LENGTH' => '55', 'DATA_TYPE' => 'TEXT']),
            '55',
        ];
        yield 'zero character_maximum_length has no length' => [
            $this->columnRow(['CHARACTER_MAXIMUM_LENGTH' => '0', 'DATA_TYPE' => 'TEXT']),
            null,
        ];
        yield 'number with scale' => [
            $this->columnRow(['NUMERIC_PRECISION' => '38', 'NUMERIC_SCALE' => '0', 'DATA_TYPE' => 'NUMBER']),
            '38,0',
        ];
        yield 'number without scale' => [
            $this->columnRow(['NUMERIC_PRECISION' => '38', 'NUMERIC_SCALE' => null, 'DATA_TYPE' => 'NUMBER']),
            '38',
        ];
        yield 'timestamp_ntz uses datetime_precision' => [
            $this->columnRow(['DATETIME_PRECISION' => '9', 'DATA_TYPE' => 'TIMESTAMP_NTZ']),
            '9',
        ];
        yield 'timestamp_tz uses datetime_precision' => [
            $this->columnRow(['DATETIME_PRECISION' => '9', 'DATA_TYPE' => 'TIMESTAMP_TZ']),
            '9',
        ];
        yield 'timestamp_ltz uses datetime_precision' => [
            $this->columnRow(['DATETIME_PRECISION' => '3', 'DATA_TYPE' => 'TIMESTAMP_LTZ']),
            '3',
        ];
        yield 'time uses datetime_precision' => [
            $this->columnRow(['DATETIME_PRECISION' => '9', 'DATA_TYPE' => 'TIME']),
            '9',
        ];
        yield 'timestamp with zero precision keeps zero' => [
            $this->columnRow(['DATETIME_PRECISION' => '0', 'DATA_TYPE' => 'TIMESTAMP_NTZ']),
            '0',
        ];
        yield 'date has no length even if datetime_precision is present' => [
            $this->columnRow(['DATETIME_PRECISION' => '0', 'DATA_TYPE' => 'DATE']),
            null,
        ];
        yield 'boolean has no length' => [
            $this->columnRow(['DATA_TYPE' => 'BOOLEAN']),
            null,
        ];
    }

    /**
     * @param array<string, mixed> $overrides
     * @return array<string, mixed>
     */
    private function columnRow(array $overrides): array
    {
        return array_merge([
            'CHARACTER_MAXIMUM_LENGTH' => null,
            'NUMERIC_PRECISION' => null,
            'NUMERIC_SCALE' => null,
            'DATETIME_PRECISION' => null,
            'DATA_TYPE' => null,
        ], $overrides);
    }
}
