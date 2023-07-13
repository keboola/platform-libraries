<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Helper;

use Keboola\InputMapping\Helper\FakeDevStorageTableRewriteHelper;
use Keboola\InputMapping\Helper\RealDevStorageTableRewriteHelper;
use Keboola\InputMapping\Helper\TableRewriteHelperFactory;
use Keboola\InputMapping\Table\Options\ReaderOptions;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use PHPUnit\Framework\TestCase;

class TableRewriteHelperFactoryTest extends TestCase
{
    public function testGetRewriteHelper(): void
    {
        $helper = TableRewriteHelperFactory::getTableRewriteHelper(new ClientOptions(
            ));
        self::assertInstanceOf(FakeDevStorageTableRewriteHelper::class, $helper);

        $helper = TableRewriteHelperFactory::getTableRewriteHelper(new ClientOptions(
            useBranchStorage: false
        ));
        self::assertInstanceOf(FakeDevStorageTableRewriteHelper::class, $helper);

        $helper = TableRewriteHelperFactory::getTableRewriteHelper(new ClientOptions(
            useBranchStorage: true
        ));
        self::assertInstanceOf(RealDevStorageTableRewriteHelper::class, $helper);
    }
}
