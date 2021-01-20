<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Utils\AccountUrlParser;
use PHPUnit\Framework\TestCase;

class AccountParseTest extends TestCase
{
    public function testParseAccount(): void
    {
        $this->assertEquals(
            'something',
            AccountUrlParser::parse('something.snowflakecomputing.com')
        );

        $this->assertEquals(
            'demo.something',
            AccountUrlParser::parse('demo.something.snowflakecomputing.com')
        );

        $this->assertEquals(
            'next.demo.something',
            AccountUrlParser::parse('next.demo.something.snowflakecomputing.com')
        );
    }
}
