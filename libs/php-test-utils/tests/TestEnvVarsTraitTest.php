<?php

declare(strict_types=1);

namespace Keboola\PhpTestUtils\Tests;

use Keboola\PhpTestUtils\TestEnvVarsTrait;
use PHPUnit\Framework\AssertionFailedError;

class TestEnvVarsTraitTest extends TestEnvVarsTraitAbstract
{
    protected function setUp(): void
    {
        parent::setUp();
    }

    private function setEnvRaw(string $name, string|int $value): void
    {
        $_ENV[$name] = $value;
        putenv(sprintf('%s=%s', $name, $value));
    }

    private function unsetEnvRaw(string $name): void
    {
        unset($_ENV[$name]);
        putenv($name);
    }

    public function testGetOptionalEnvReturnsNullWhenNotSet(): void
    {
        $name = 'TEST_VAR_OPT_NULL';
        $this->unsetEnvRaw($name);

        $value = self::getOptionalEnv($name);
        self::assertNull($value);
    }

    public function testGetOptionalEnvReturnsValueWhenSet(): void
    {
        $name = 'TEST_VAR_OPT_VALUE';
        $this->setEnvRaw($name, 'foo');

        $value = self::getOptionalEnv($name);
        self::assertSame('foo', $value);
    }

    public function testGetOptionalEnvReturnsStringValueWhenSet(): void
    {
        $name = 'TEST_VAR_OPT_VALUE';
        $this->setEnvRaw($name, 1);

        $value = self::getOptionalEnv($name);
        self::assertSame('1', $value);
    }

    public function testGetRequiredEnvReturnsValueWhenSet(): void
    {
        $name = 'TEST_VAR_REQ_VALUE';
        $this->setEnvRaw($name, 'bar');

        $value = self::getRequiredEnv($name);
        self::assertSame('bar', $value);
    }

    public function testGetRequiredEmptyName(): void
    {
        $this->expectException(AssertionFailedError::class);
        $this->expectExceptionMessage('Environment variable "" is not set.');
        self::getRequiredEnv('');
    }

    public function testGetRequiredEnvFailsWhenMissing(): void
    {
        $name = 'TEST_VAR_REQ_MISSING';
        $this->unsetEnvRaw($name);

        $this->expectException(AssertionFailedError::class);
        self::getRequiredEnv($name);
    }

    public function testOverrideEnvSetsAndUnsets(): void
    {
        $name = 'TEST_VAR_OVERRIDE';

        self::overrideEnv($name, 'baz');
        self::assertSame('baz', getenv($name));
        self::assertSame('baz', $_ENV[$name]);

        self::overrideEnv($name, null);
        self::assertFalse(getenv($name));
        self::assertArrayNotHasKey($name, $_ENV);
    }
}
