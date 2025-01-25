<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\Source;

use Keboola\OutputMapping\Exception\InvalidOutputException;
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

    public function getWorkspaceId(): string
    {
        throw new InvalidOutputException('Not implemented');
    }

    public function getDataObject(): string
    {
        throw new InvalidOutputException('Not implemented');
    }

    public function getSourceType(): SourceType
    {
        return SourceType::LOCAL;
    }
}
