<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient\Tests\Model;

use Keboola\GitServiceApiClient\Model\GitRefListWrapper;
use PHPUnit\Framework\TestCase;
use Webmozart\Assert\InvalidArgumentException;

class GitRefListWrapperTest extends TestCase
{
    public function testFromResponseData(): void
    {
        $wrapper = GitRefListWrapper::fromResponseData([
            'refs' => [
                ['ref' => 'refs/heads/main', 'sha' => 'abc', 'type' => 'commit'],
                ['ref' => 'refs/tags/v1', 'sha' => 'def', 'type' => 'tag'],
            ],
        ]);

        self::assertCount(2, $wrapper->refs);
        self::assertSame('refs/heads/main', $wrapper->refs[0]->ref);
        self::assertSame('tag', $wrapper->refs[1]->type);
    }

    public function testFromResponseDataEmpty(): void
    {
        $wrapper = GitRefListWrapper::fromResponseData(['refs' => []]);

        self::assertSame([], $wrapper->refs);
    }

    public function testFromResponseDataMissingRefs(): void
    {
        $this->expectException(InvalidArgumentException::class);
        GitRefListWrapper::fromResponseData([]);
    }
}
