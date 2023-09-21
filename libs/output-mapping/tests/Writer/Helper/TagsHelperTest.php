<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Helper;

use Keboola\OutputMapping\Tests\Writer\CreateBranchTrait;
use Keboola\OutputMapping\Writer\Helper\TagsHelper;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use PHPUnit\Framework\TestCase;

class TagsHelperTest extends TestCase
{
    use CreateBranchTrait;

    private function getStorageConfig(): array
    {
        return [
            'tags' => [
                'first-tag',
                'secondary-tag',
            ],
            'is_public' => true,
            'is_permanent' => false,
            'is_encrypted' => true,
            'notify' => false,
        ];
    }

    public function testRewriteTags(): void
    {
        $clientWrapper = $this->getClientWrapper(null);
        $branchId = $this->createBranch($clientWrapper, 'dev 123');
        $clientWrapper = $this->getClientWrapper($branchId);
        $storageConfig = $this->getStorageConfig();
        $expectedConfig = TagsHelper::rewriteTags($storageConfig, $clientWrapper);
        self::assertEquals(
            [
                sprintf('%s-first-tag', $branchId),
                sprintf('%s-secondary-tag', $branchId),
            ],
            $expectedConfig['tags'],
        );
        unset($expectedConfig['tags']);
        unset($storageConfig['tags']);
        self::assertEquals($storageConfig, $expectedConfig);
    }

    public function testRewriteEmptyTags(): void
    {
        $clientWrapper = $this->getClientWrapper(null);
        $branchId = $this->createBranch($clientWrapper, 'dev 123');
        $clientWrapper = $this->getClientWrapper($branchId);
        $storageConfig = $this->getStorageConfig();
        $storageConfig['tags'] = [];
        $expectedConfig = TagsHelper::rewriteTags($storageConfig, $clientWrapper);
        self::assertEquals([], $expectedConfig['tags']);
        unset($expectedConfig['tags']);
        unset($storageConfig['tags']);
        self::assertEquals($storageConfig, $expectedConfig);
    }

    protected function getClientWrapper(?string $branchId): ClientWrapper
    {
        return new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('STORAGE_API_TOKEN_MASTER'),
                $branchId,
            ),
        );
    }

    public function testRewriteNoBranch(): void
    {
        $clientWrapper = $this->getClientWrapper(null);
        $storageConfig = $this->getStorageConfig();
        $expectedConfig = TagsHelper::rewriteTags($storageConfig, $clientWrapper);
        self::assertEquals(
            [
                'first-tag',
                'secondary-tag',
            ],
            $expectedConfig['tags'],
        );
        unset($expectedConfig['tags']);
        unset($storageConfig['tags']);
        self::assertEquals($storageConfig, $expectedConfig);
    }
}
