<?php

namespace Keboola\OutputMapping\Tests\Writer;

class StorageApiWriterMetadataRedshiftTest extends BaseWriterMetadataTest
{
    private const INPUT_BUCKET = 'in.c-StorageApiSlicedWriterRedshiftTest';
    private const FILE_TAG = 'StorageApiSlicedWriterRedshiftTest';

    public function setUp()
    {
        parent::setUp();
        $this->clearBuckets([self::INPUT_BUCKET]);
        $this->clearFileUploads([self::FILE_TAG]);
        $this->clientWrapper->getBasicClient()->createBucket(
            'StorageApiSlicedWriterRedshiftTest',
            "in",
            '',
            'redshift'
        );
    }

    public function testMetadataWritingTestColumnChange()
    {
        $this->metadataWritingTestColumnChangeTest(self::INPUT_BUCKET);
    }

    public function testMetadataWritingTestColumnChangeSpecialDelimiter()
    {
        $this->metadataWritingTestColumnChangeSpecialDelimiter(self::INPUT_BUCKET);
    }

    public function testMetadataWritingTestColumnChangeSpecialChars()
    {
        $this->metadataWritingTestColumnChangeSpecialChars(self::INPUT_BUCKET);
    }

    public function testMetadataWritingTestColumnChangeHeadless()
    {
        $this->metadataWritingTestColumnChangeHeadless(self::INPUT_BUCKET);
    }
}
