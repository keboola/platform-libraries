<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Redshift;

use Keboola\OutputMapping\Tests\Writer\BaseWriterMetadataTest;

class StorageApiWriterMetadataRedshiftTest extends BaseWriterMetadataTest
{
    private const INPUT_BUCKET = 'in.c-StorageApiSlicedWriterRedshiftTest';
    private const FILE_TAG = 'StorageApiSlicedWriterRedshiftTest';

    public function setUp(): void
    {
        parent::setUp();
        $this->clearBuckets([self::INPUT_BUCKET]);
        $this->clearFileUploads([self::FILE_TAG]);
        $this->clientWrapper->getBasicClient()->createBucket(
            'StorageApiSlicedWriterRedshiftTest',
            'in',
            '',
            'redshift'
        );
    }

    public function testMetadataWritingTestColumnChange(): void
    {
        $this->metadataWritingTestColumnChangeTest(self::INPUT_BUCKET);
    }

    public function testMetadataWritingTestColumnChangeSpecialDelimiter(): void
    {
        $this->metadataWritingTestColumnChangeSpecialDelimiter(self::INPUT_BUCKET);
    }

    public function testMetadataWritingTestColumnChangeSpecialChars(): void
    {
        $this->metadataWritingTestColumnChangeSpecialChars(self::INPUT_BUCKET);
    }

    public function testMetadataWritingTestColumnChangeHeadless(): void
    {
        $this->metadataWritingTestColumnChangeHeadless(self::INPUT_BUCKET);
    }
}
