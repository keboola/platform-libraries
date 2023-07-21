<?php

declare(strict_types=1);

namespace Keboola\InputMapping\File\Options;

class RewrittenInputFileOptionsList
{
    /**
     * @var RewrittenInputFileOptions[]
     */
    private array $files = [];

    /**
     * @param RewrittenInputFileOptions[] $fileOptionsList
     */
    public function __construct(array $fileOptionsList)
    {
        $this->files = $fileOptionsList;
    }

    /**
     * @returns RewrittenInputFileOptions[]
     */
    public function getFiles(): array
    {
        return $this->files;
    }
}
