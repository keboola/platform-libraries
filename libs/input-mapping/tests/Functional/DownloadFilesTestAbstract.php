<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\InputMapping\Tests\AbstractTestCase;
use Keboola\StorageApi\Options\ListFilesOptions;
use Symfony\Component\Filesystem\Filesystem;

class DownloadFilesTestAbstract extends AbstractTestCase
{
    protected const TEST_FILE_TAG_FOR_BRANCH = 'testReadFilesForBranch';
    protected const DEFAULT_TEST_FILE_TAG = 'download-files-test';

    public function setUp(): void
    {
        parent::setUp();

        // Delete file uploads
        sleep(4);
        $options = new ListFilesOptions();
        $options->setTags([self::DEFAULT_TEST_FILE_TAG, self::TEST_FILE_TAG_FOR_BRANCH]);
        $options->setLimit(1000);
        $files = $this->clientWrapper->getBasicClient()->listFiles($options);
        foreach ($files as $file) {
            $this->clientWrapper->getBasicClient()->deleteFile($file['id']);
        }
        sleep(2);
    }
}
