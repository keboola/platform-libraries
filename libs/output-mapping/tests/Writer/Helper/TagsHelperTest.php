<?php

namespace Keboola\OutputMapping\Tests\Writer\Helper;

use Keboola\OutputMapping\Tests\Writer\CreateBranchTrait;
use Keboola\OutputMapping\Writer\Helper\TagsHelper;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use PHPUnit\Framework\TestCase;

class TagsHelperTest extends TestCase
{
    use CreateBranchTrait;

    private function getStorageConfig()
    {
        return [
            'tags' => [
                'first-tag',
                'secondary-tag'
            ],
            'is_public' => true,
            'is_permanent' => false,
            'is_encrypted' => true,
            'notify' => false,
        ];
    }

    public function testRewriteTags()
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
            $expectedConfig['tags']
        );
        unset($expectedConfig['tags']);
        unset($storageConfig['tags']);
        self::assertEquals($storageConfig, $expectedConfig);
    }

    public function testRewriteEmptyTags()
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
            new ClientOptions(STORAGE_API_URL, STORAGE_API_TOKEN_MASTER, $branchId),
        );
    }

    public function testRewriteNoBranch()
    {
        $clientWrapper = $this->getClientWrapper(null);
        $storageConfig = $this->getStorageConfig();
        $expectedConfig = TagsHelper::rewriteTags($storageConfig, $clientWrapper);
        self::assertEquals(
            [
                'first-tag',
                'secondary-tag',
            ],
            $expectedConfig['tags']
        );
        unset($expectedConfig['tags']);
        unset($storageConfig['tags']);
        self::assertEquals($storageConfig, $expectedConfig);
    }
}
