<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Helper;

use Keboola\InputMapping\Table\Options\ReaderOptions;

class TableRewriteHelperFactory
{
    public static function getTableRewriteHelper(ReaderOptions $readerOptions): TableRewriteHelperInterface
    {
        if ($readerOptions->hasProtectedDefaultBranch()) {
            return new RealDevStorageTableRewriteHelper();
        } else {
            return new FakeDevStorageTableRewriteHelper();
        }
    }
}
