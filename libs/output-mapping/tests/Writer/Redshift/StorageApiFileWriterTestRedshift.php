<?php

namespace Keboola\OutputMapping\Tests\Writer;

class StorageApiFileWriterTestRedshift extends BaseWriterTest
{
    use CreateBranchTrait;

    const DEFAULT_SYSTEM_METADATA = ['componentId' => 'foo'];

    public function setUp()
    {
        parent::setUp();
        $this->clearFileUploads(['output-mapping-bundle-test']);
        $this->clearBuckets([
            'out.c-output-mapping-test',
            'out.c-output-mapping-default-test',
            'out.c-output-mapping-redshift-test',
            'in.c-output-mapping-test',
            'out.c-dev-123-output-mapping-test'
        ]);
        $this->clientWrapper->getBasicClient()->createBucket('output-mapping-redshift-test', 'out', '', 'redshift');
        $this->clientWrapper->getBasicClient()->createBucket('output-mapping-default-test', 'out');
    }
}
