<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient\Tests;

use Keboola\GitServiceApiClient\GitServiceErrorMessageResolver;
use PHPUnit\Framework\TestCase;

class GitServiceErrorMessageResolverTest extends TestCase
{
    private GitServiceErrorMessageResolver $resolver;

    protected function setUp(): void
    {
        $this->resolver = new GitServiceErrorMessageResolver();
    }

    public function testReturnsFormattedMessageWhenCodeAndErrorPresent(): void
    {
        $body = (string) json_encode(['code' => 'repository.notFound', 'error' => 'repo missing']);
        $result = ($this->resolver)($body, 404);

        self::assertSame('repository.notFound: repo missing', $result);
    }

    public function testReturnsNullWhenBodyIsNotJson(): void
    {
        $result = ($this->resolver)('plain text error', 400);

        self::assertNull($result);
    }

    public function testReturnsNullWhenCodeMissing(): void
    {
        $body = (string) json_encode(['error' => 'something went wrong']);
        $result = ($this->resolver)($body, 500);

        self::assertNull($result);
    }

    public function testReturnsNullWhenErrorMissing(): void
    {
        $body = (string) json_encode(['code' => 'some.code']);
        $result = ($this->resolver)($body, 500);

        self::assertNull($result);
    }

    public function testReturnsNullWhenCodeIsEmpty(): void
    {
        $body = (string) json_encode(['code' => '', 'error' => 'repo missing']);
        $result = ($this->resolver)($body, 404);

        self::assertNull($result);
    }

    public function testReturnsNullWhenErrorIsEmpty(): void
    {
        $body = (string) json_encode(['code' => 'repository.notFound', 'error' => '']);
        $result = ($this->resolver)($body, 404);

        self::assertNull($result);
    }

    public function testReturnsNullWhenCodeIsNotString(): void
    {
        $body = (string) json_encode(['code' => 42, 'error' => 'repo missing']);
        $result = ($this->resolver)($body, 404);

        self::assertNull($result);
    }

    public function testReturnsNullWhenBodyIsEmptyJson(): void
    {
        $result = ($this->resolver)('{}', 500);

        self::assertNull($result);
    }

    public function testTrimsWhitespaceFromMessage(): void
    {
        $body = (string) json_encode(['code' => ' repo.error ', 'error' => ' bad request ']);
        $result = ($this->resolver)($body, 400);

        // trim is applied to the concatenated string
        self::assertSame('repo.error :  bad request', $result);
    }
}
