<?php
namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Utils\AccountUrlParser;

class AccountParseTest extends \PHPUnit_Framework_TestCase
{
    public function testParseAccount()
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