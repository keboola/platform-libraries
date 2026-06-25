<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient\Tests\Model;

use Keboola\GitServiceApiClient\Model\CommitList;
use PHPUnit\Framework\TestCase;
use Webmozart\Assert\InvalidArgumentException;

class CommitListTest extends TestCase
{
    public function testFromResponseData(): void
    {
        $list = CommitList::fromResponseData([
            'commits' => [
                [
                    'sha' => 'abc123',
                    'created' => 't',
                    'message' => 'm',
                    'author' => ['name' => 'A', 'email' => 'a@b.c', 'date' => 't'],
                ],
            ],
            'total' => 42,
        ]);

        self::assertCount(1, $list->commits);
        self::assertSame('abc123', $list->commits[0]->sha);
        self::assertSame(42, $list->total);
    }

    public function testFromResponseDataEmpty(): void
    {
        $list = CommitList::fromResponseData([
            'commits' => [],
            'total' => 0,
        ]);

        self::assertSame([], $list->commits);
        self::assertSame(0, $list->total);
    }

    public function testFromResponseDataMissingTotal(): void
    {
        $this->expectException(InvalidArgumentException::class);
        CommitList::fromResponseData([
            'commits' => [],
        ]);
    }

    public function testFromResponseDataTotalWrongType(): void
    {
        $this->expectException(InvalidArgumentException::class);
        CommitList::fromResponseData([
            'commits' => [],
            'total' => '42',
        ]);
    }
}
