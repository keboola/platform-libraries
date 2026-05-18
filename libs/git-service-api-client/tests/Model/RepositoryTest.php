<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient\Tests\Model;

use Keboola\GitServiceApiClient\Model\Repository;
use PHPUnit\Framework\TestCase;
use Webmozart\Assert\InvalidArgumentException;

class RepositoryTest extends TestCase
{
    public function testFromResponseData(): void
    {
        $repo = Repository::fromResponseData([
            'name' => 'app-123',
            'createdAt' => '2026-04-28T10:00:00Z',
            'defaultBranch' => 'main',
            'sshUrl' => 'ssh://git@git-service/app-123.git',
            'httpsUrl' => 'https://git-service/app-123.git',
        ]);

        self::assertSame('app-123', $repo->name);
        self::assertSame('2026-04-28T10:00:00Z', $repo->createdAt);
        self::assertSame('main', $repo->defaultBranch);
        self::assertSame('ssh://git@git-service/app-123.git', $repo->sshUrl);
        self::assertSame('https://git-service/app-123.git', $repo->httpsUrl);
    }

    public function testFromResponseDataMissingHttpsUrl(): void
    {
        $this->expectException(InvalidArgumentException::class);
        Repository::fromResponseData([
            'name' => 'app-123',
            'createdAt' => '2026-04-28T10:00:00Z',
            'defaultBranch' => 'main',
            'sshUrl' => 'ssh://git@git-service/app-123.git',
            // httpsUrl missing
        ]);
    }

    public function testFromResponseDataHttpsUrlWrongType(): void
    {
        $this->expectException(InvalidArgumentException::class);
        Repository::fromResponseData([
            'name' => 'app-123',
            'createdAt' => '2026-04-28T10:00:00Z',
            'defaultBranch' => 'main',
            'sshUrl' => 'ssh://git@git-service/app-123.git',
            'httpsUrl' => 123,
        ]);
    }

    public function testFromResponseDataMissingField(): void
    {
        $this->expectException(InvalidArgumentException::class);
        Repository::fromResponseData([
            'name' => 'app-123',
            'createdAt' => '2026-04-28T10:00:00Z',
            'defaultBranch' => 'main',
            // sshUrl missing
        ]);
    }

    public function testFromResponseDataWrongType(): void
    {
        $this->expectException(InvalidArgumentException::class);
        Repository::fromResponseData([
            'name' => 123,
            'createdAt' => '2026-04-28T10:00:00Z',
            'defaultBranch' => 'main',
            'sshUrl' => 'ssh://git@git-service/app-123.git',
        ]);
    }
}
