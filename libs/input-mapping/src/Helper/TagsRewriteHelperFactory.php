<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Helper;

use Keboola\StorageApiBranch\Factory\ClientOptions;

class TagsRewriteHelperFactory
{
    public static function getTagsRewriteHelper(ClientOptions $clientOptions): TagsRewriteHelperInterface
    {
        if ($clientOptions->useBranchStorage()) {
            return new RealDevStorageTagsRewriteHelper();
        } else {
            return new FakeDevStorageTagsRewriteHelper();
        }
    }
}
