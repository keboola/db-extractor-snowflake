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
    }
}
