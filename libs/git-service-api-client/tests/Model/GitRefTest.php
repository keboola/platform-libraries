<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient\Tests\Model;

use Keboola\GitServiceApiClient\Model\GitRef;
use PHPUnit\Framework\TestCase;
use Webmozart\Assert\InvalidArgumentException;

class GitRefTest extends TestCase
{
    public function testFromResponseData(): void
    {
        $ref = GitRef::fromResponseData([
            'ref' => 'refs/heads/main',
            'sha' => 'abc123',
            'type' => 'commit',
        ]);

        self::assertSame('refs/heads/main', $ref->ref);
        self::assertSame('abc123', $ref->sha);
        self::assertSame('commit', $ref->type);
    }

    public function testFromResponseDataMissingField(): void
    {
        $this->expectException(InvalidArgumentException::class);
        GitRef::fromResponseData([
            'ref' => 'refs/heads/main',
            'sha' => 'abc123',
            // type missing
        ]);
    }

    public function testFromResponseDataWrongType(): void
    {
        $this->expectException(InvalidArgumentException::class);
        GitRef::fromResponseData([
            'ref' => 'refs/heads/main',
            'sha' => 123,
            'type' => 'commit',
        ]);
    }
}
