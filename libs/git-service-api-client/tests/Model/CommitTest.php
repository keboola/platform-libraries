<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient\Tests\Model;

use Keboola\GitServiceApiClient\Model\Commit;
use PHPUnit\Framework\TestCase;
use Webmozart\Assert\InvalidArgumentException;

class CommitTest extends TestCase
{
    public function testFromResponseData(): void
    {
        $commit = Commit::fromResponseData([
            'sha' => 'abc123',
            'created' => '2026-06-01T10:00:00Z',
            'message' => 'commit message',
            'author' => [
                'name' => 'Alice',
                'email' => 'alice@example.com',
                'date' => '2026-06-01T10:00:00Z',
            ],
        ]);

        self::assertSame('abc123', $commit->sha);
        self::assertSame('2026-06-01T10:00:00Z', $commit->created);
        self::assertSame('commit message', $commit->message);
        self::assertSame('Alice', $commit->author->name);
        self::assertSame('alice@example.com', $commit->author->email);
        self::assertSame('2026-06-01T10:00:00Z', $commit->author->date);
    }

    public function testFromResponseDataMissingField(): void
    {
        $this->expectException(InvalidArgumentException::class);
        Commit::fromResponseData([
            'sha' => 'abc123',
            'created' => '2026-06-01T10:00:00Z',
            'message' => 'commit message',
            // author missing
        ]);
    }

    public function testFromResponseDataAuthorWrongType(): void
    {
        $this->expectException(InvalidArgumentException::class);
        Commit::fromResponseData([
            'sha' => 'abc123',
            'created' => '2026-06-01T10:00:00Z',
            'message' => 'commit message',
            'author' => 'not-an-array',
        ]);
    }

    public function testFromResponseDataWrongType(): void
    {
        $this->expectException(InvalidArgumentException::class);
        Commit::fromResponseData([
            'sha' => 123,
            'created' => '2026-06-01T10:00:00Z',
            'message' => 'commit message',
            'author' => ['name' => 'Alice', 'email' => 'a@b.c', 'date' => 't'],
        ]);
    }
}
