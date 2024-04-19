<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Storage;

use Keboola\OutputMapping\Storage\BucketInfo;
use PHPUnit\Framework\TestCase;

class BucketInfoTest extends TestCase
{
    private BucketInfo $bucketInfo;

    protected function setUp(): void
    {
        $this->bucketInfo = new BucketInfo([
            'id' => 'bucketId',
            'backend' => 'backendType',
        ]);
    }

    public function testBasic(): void
    {
        $this->assertEquals('bucketId', $this->bucketInfo->id);
        $this->assertEquals('backendType', $this->bucketInfo->backend);
    }
}
