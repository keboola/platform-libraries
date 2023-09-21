<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\File;

class FileItem
{
    /**
     * @param string $name Generalized name (for ABS this can include slashes)
     * @param string $path Generalized path (for ABS this can be an URL)
     */
    public function __construct(
        private readonly string $pathName,
        private readonly string $path,
        private readonly string $name,
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
}
