<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Workspace;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Workspace\Workspace;
use Keboola\StorageApi\WorkspaceLoginType;
use PHPUnit\Framework\TestCase;

class WorkspaceTest extends TestCase
{
    public function testFromDataArray(): void
    {
        $workspace = Workspace::createFromData([
            'id' => '123456',
            'connection' => [
                'backend' => 'bigquery',
            ],
        ]);

        self::assertSame('123456', $workspace->getWorkspaceId());
        self::assertSame('bigquery', $workspace->getBackendType());
        self::assertNull($workspace->getBackendSize());
        self::assertSame(WorkspaceLoginType::DEFAULT, $workspace->getLoginType());
    }

    public function testFromInvalidDataArray(): void
    {
        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage('Invalid workspace data: ');

        Workspace::createFromData([]);
    }

    public function testFromDataArrayWithCustomLoginType(): void
    {
        $workspace = Workspace::createFromData([
            'id' => '123456',
            'connection' => [
                'backend' => 'snowflake',
                'loginType' => WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR->value,
            ],
        ]);

        self::assertSame(WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR, $workspace->getLoginType());
    }

    public function testFromDataArrayWithBackendSize(): void
    {
        $workspace = Workspace::createFromData([
            'id' => '123456',
            'backendSize' => 'small',
            'connection' => [
                'backend' => 'snowflake',
            ],
        ]);

        self::assertSame('small', $workspace->getBackendSize());
    }
}
