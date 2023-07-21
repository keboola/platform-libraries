<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Helper;

use Keboola\InputMapping\Helper\FakeDevStorageTagsRewriteHelper;
use Keboola\InputMapping\Helper\RealDevStorageTagsRewriteHelper;
use Keboola\InputMapping\Helper\TagsRewriteHelperFactory;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use PHPUnit\Framework\TestCase;

class TagsRewriteHelperFactoryTest extends TestCase
{
    public function testGetRewriteHelper(): void
    {
        $helper = TagsRewriteHelperFactory::getTagsRewriteHelper(new ClientOptions());
        self::assertInstanceOf(FakeDevStorageTagsRewriteHelper::class, $helper);

        $helper = TagsRewriteHelperFactory::getTagsRewriteHelper(new ClientOptions(
            useBranchStorage: false
        ));
        self::assertInstanceOf(FakeDevStorageTagsRewriteHelper::class, $helper);

        $helper = TagsRewriteHelperFactory::getTagsRewriteHelper(new ClientOptions(
            useBranchStorage: true
        ));
        self::assertInstanceOf(RealDevStorageTagsRewriteHelper::class, $helper);
    }
}
