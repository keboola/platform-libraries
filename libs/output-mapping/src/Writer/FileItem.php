<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Writer\Table\Source\SourceInterface;
use Keboola\OutputMapping\Writer\Table\Source\SourceType;

class FileItem implements SourceInterface
{
    /**
     * @param string $name Generalized name (for ABS this can include slashes)
     * @param string $path Generalized path (for ABS this can be an URL)
     */
    public function __construct(
        private readonly string $pathName,
        private readonly string $path,
        private readonly string $name,
        private readonly bool $isSliced,
    ) {
    }

    public function getPathName(): string
    {
        return $this->pathName;
    }

    public function getPath(): string
    {
        return $this->path;
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function isSliced(): bool
    {
        return $this->isSliced;
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
        return SourceType::FILE;
    }
}
