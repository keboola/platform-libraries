<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Helper;

use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Needs\NeedsDevBranch;
use Keboola\OutputMapping\Writer\Helper\TagsHelper;

class TagsHelperTest extends AbstractTestCase
{
    private const STORAGE_CONFIG = [
        'tags' => [
            'first-tag',
            'secondary-tag',
        ],
        'is_public' => true,
        'is_permanent' => false,
        'is_encrypted' => true,
        'notify' => false,
    ];

    #[NeedsDevBranch]
    public function testRewriteTags(): void
    {
        $this->initClient($this->devBranchId);

        $storageConfig = self::STORAGE_CONFIG;
        $expectedConfig = TagsHelper::rewriteTags($storageConfig, $this->clientWrapper);
        self::assertEquals(
            [
                sprintf('%s-first-tag', $this->devBranchId),
                sprintf('%s-secondary-tag', $this->devBranchId),
            ],
            $expectedConfig['tags'],
        );
        unset($expectedConfig['tags']);
        unset($storageConfig['tags']);
        self::assertEquals($storageConfig, $expectedConfig);
    }

    #[NeedsDevBranch]
    public function testRewriteEmptyTags(): void
    {
        $this->initClient($this->devBranchId);

        $storageConfig = self::STORAGE_CONFIG;
        $storageConfig['tags'] = [];
        $expectedConfig = TagsHelper::rewriteTags($storageConfig, $this->clientWrapper);
        self::assertEquals([], $expectedConfig['tags']);
        unset($expectedConfig['tags']);
        unset($storageConfig['tags']);
        self::assertEquals($storageConfig, $expectedConfig);
    }

    public function testRewriteNoBranch(): void
    {
        $storageConfig = self::STORAGE_CONFIG;
        $expectedConfig = TagsHelper::rewriteTags($storageConfig, $this->clientWrapper);
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
