<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver\Tests\VariablesRenderer;

use Keboola\ConfigurationVariablesResolver\Exception\UserException;
use Keboola\ConfigurationVariablesResolver\VariablesRenderer\VariablesRenderer;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

class VariablesRendererTest extends TestCase
{
    private readonly TestHandler $logsHandler;
    private readonly LoggerInterface $logger;

    public function setUp(): void
    {
        parent::setUp();

        $this->logsHandler = new TestHandler();
        $this->logger = new Logger('tests', [$this->logsHandler]);
    }

    public function testRenderVariables(): void
    {
        $renderer = new VariablesRenderer($this->logger);
        $configuration = $renderer->renderVariables(
            [
                'parameters' => [
                    'param' => 'foo is {{ foo }}, goo is {{ goo }}',
                    'other' => '{{ foo }}',
                ],
            ],
            [
                'foo' => 'bar',
                'goo' => 'gar',
            ]
        );

        self::assertSame(
            [
                'parameters' => [
                    'param' => 'foo is bar, goo is gar',
                    'other' => 'bar',
                ],
            ],
            $configuration,
        );
        self::assertTrue($this->logsHandler->hasInfoThatContains('Replaced values for variables: foo, goo'));
    }

    public function testResolveMissingVariable(): void
    {
        $this->expectException(UserException::class);
        $this->expectExceptionMessage('Missing values for placeholders: key2, key3');

        $renderer = new VariablesRenderer($this->logger);
        $renderer->renderVariables(
            [
                'parameters' => [
                    'param' => '{{ key1 }} {{ key2 }} {{ key3 }}',
                ],
            ],
            [
                'key1' => 'val1',
            ]
        );
    }

    public function testResolveVariablesSpecialCharacterReplacement(): void
    {
        $renderer = new VariablesRenderer($this->logger);
        $configuration = $renderer->renderVariables(
            [
                'parameters' => [
                    'param' => 'foo is {{ foo }}',
                ],
            ],
            [
                'foo' => 'special " \' { } characters',
            ]
        );

        self::assertSame(
            [
                'parameters' => [
                    'param' => 'foo is special " \' { } characters',
                ],
            ],
            $configuration,
        );
        self::assertTrue($this->logsHandler->hasInfoThatContains('Replaced values for variables: foo'));
    }

    public function testResolveValueEndingWithQuote(): void
    {
        $renderer = new VariablesRenderer($this->logger);
        $configuration = $renderer->renderVariables(
            [
                'parameters' => [
                    'param' => '{{ key1 }}',
                ],
            ],
            [
                'key1' => '"',
            ]
        );

        self::assertSame(
            [
                'parameters' => [
                    'param' => '"',
                ],
            ],
            $configuration,
        );
    }

    public function testResolveJsonBreakingValue(): void
    {
        $this->expectException(UserException::class);
        $this->expectExceptionMessage(
            'Variable replacement resulted in invalid configuration, error: ' .
            'Control character error, possibly incorrectly encoded',
        );

        $renderer = new VariablesRenderer($this->logger);
        $renderer->renderVariables(
            [
                'parameters' => [
                    'param' => '{{{ key1 }}}',
                ],
            ],
            [
                'key1' => 'value"',
            ]
        );
    }
}
