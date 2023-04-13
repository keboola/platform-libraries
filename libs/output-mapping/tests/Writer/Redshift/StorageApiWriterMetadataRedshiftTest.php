<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Redshift;

use Keboola\OutputMapping\Tests\Needs\NeedsEmptyRedshiftOutputBucket;
use Keboola\OutputMapping\Tests\Writer\BaseWriterMetadataTest;

class StorageApiWriterMetadataRedshiftTest extends BaseWriterMetadataTest
{
    #[NeedsEmptyRedshiftOutputBucket]
    public function testMetadataWritingTestColumnChange(): void
    {
        $this->metadataWritingTestColumnChangeTest($this->emptyRedshiftOutputBucketId);
    }

    #[NeedsEmptyRedshiftOutputBucket]
    public function testMetadataWritingTestColumnChangeSpecialDelimiter(): void
    {
        $this->metadataWritingTestColumnChangeSpecialDelimiter($this->emptyRedshiftOutputBucketId);
    }

    #[NeedsEmptyRedshiftOutputBucket]
    public function testMetadataWritingTestColumnChangeSpecialChars(): void
    {
        $this->metadataWritingTestColumnChangeSpecialChars($this->emptyRedshiftOutputBucketId);
    }

    #[NeedsEmptyRedshiftOutputBucket]
    public function testMetadataWritingTestColumnChangeHeadless(): void
    {
        $this->metadataWritingTestColumnChangeHeadless($this->emptyRedshiftOutputBucketId);
    }
}
