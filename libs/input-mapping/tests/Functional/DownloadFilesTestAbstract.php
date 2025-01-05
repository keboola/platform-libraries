<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\InputMapping\Tests\AbstractTestCase;

class DownloadFilesTestAbstract extends AbstractTestCase
{
    protected string $testFileTag;
    protected string $testFileTagForBranch;

    public function setUp(): void
    {
        parent::setUp();

        $this->testFileTag = $this->getFileTag();
        $this->testFileTagForBranch = $this->getFileTag('-branch');

        $this->clearFileUploads([$this->testFileTag, $this->testFileTagForBranch]);
    }

    protected function getFileTag(string $suffix = ''): string
    {
        $tag = $this->getName(false);
        $dataName = (string) $this->dataName();

        if ($dataName) {
            $tag .= '-' . $dataName;
        }

        return $tag . $suffix;
    }
}
