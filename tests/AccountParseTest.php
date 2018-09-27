<?php

declare(strict_types=1);

namespace Keboola\Test;

use Keboola\DbExtractor\Utils\AccountUrlParser;

class AccountParseTest extends \PHPUnit_Framework_TestCase
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
