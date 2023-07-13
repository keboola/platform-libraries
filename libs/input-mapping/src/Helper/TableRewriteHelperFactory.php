<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Helper;

use Keboola\StorageApiBranch\Factory\ClientOptions;

class TableRewriteHelperFactory
{
    public static function getTableRewriteHelper(ClientOptions $clientOptions): TableRewriteHelperInterface
    {
        if ($clientOptions->useBranchStorage()) {
            return new RealDevStorageTableRewriteHelper();
        } else {
            return new FakeDevStorageTableRewriteHelper();
        }
    }
}
