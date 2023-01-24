<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\Source;

use SplFileInfo;

class LocalFileSource implements SourceInterface
{
    private SplFileInfo $file;

    public function __construct(SplFileInfo $file)
    {
        $this->file = $file;
    }

    public function getFile(): SplFileInfo
    {
        return $this->file;
    }

    public function getName(): string
    {
        return $this->file->getBasename();
    }

    public function isSliced(): bool
    {
        return $this->file->isDir();
    }
}
