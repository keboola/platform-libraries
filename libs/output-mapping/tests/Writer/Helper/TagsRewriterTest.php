<?php

namespace Keboola\OutputMapping\Tests\Writer\Helper;

use Keboola\OutputMapping\Tests\Writer\CreateBranchTrait;
use Keboola\OutputMapping\Writer\Helper\TagsRewriter;
use Keboola\StorageApi\Client;
use Keboola\StorageApiBranch\ClientWrapper;
use PHPUnit\Framework\TestCase;

class TagsRewriterTest extends TestCase
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
        $clientWrapper = new ClientWrapper(
            new Client([
                'url' => STORAGE_API_URL,
                'token' => STORAGE_API_TOKEN_MASTER,
                'backoffMaxTries' => 1,
                'jobPollRetryDelay' => function () {
                    return 1;
                },
            ]),
            null,
            null
        );
        $clientWrapper->setBranchId($this->createBranch($clientWrapper, 'dev 123'));
        $storageConfig = $this->getStorageConfig();
        $expectedConfig = TagsRewriter::rewriteTags($storageConfig, $clientWrapper);
        self::assertEquals(
            [
                'dev-123-first-tag',
                'dev-123-secondary-tag',
            ],
            $expectedConfig['tags']
        );
        unset($expectedConfig['tags']);
        unset($storageConfig['tags']);
        self::assertEquals($storageConfig, $expectedConfig);
    }

    public function testRewriteEmptyTags()
    {
        $clientWrapper = new ClientWrapper(
            new Client([
                'url' => STORAGE_API_URL,
                'token' => STORAGE_API_TOKEN_MASTER,
                'backoffMaxTries' => 1,
                'jobPollRetryDelay' => function () {
                    return 1;
                },
            ]),
            null,
            null
        );
        $clientWrapper->setBranchId($this->createBranch($clientWrapper, 'dev 123'));
        $storageConfig = $this->getStorageConfig();
        $storageConfig['tags'] = [];
        $expectedConfig = TagsRewriter::rewriteTags($storageConfig, $clientWrapper);
        self::assertEquals([], $expectedConfig['tags']);
        unset($expectedConfig['tags']);
        unset($storageConfig['tags']);
        self::assertEquals($storageConfig, $expectedConfig);
    }

    public function testRewriteNoBranch()
    {
        $clientWrapper = new ClientWrapper(
            new Client([
                'url' => STORAGE_API_URL,
                'token' => STORAGE_API_TOKEN_MASTER,
                'backoffMaxTries' => 1,
                'jobPollRetryDelay' => function () {
                    return 1;
                },
            ]),
            null,
            null
        );
        $clientWrapper->setBranchId('');
        $storageConfig = $this->getStorageConfig();
        $expectedConfig = TagsRewriter::rewriteTags($storageConfig, $clientWrapper);
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
