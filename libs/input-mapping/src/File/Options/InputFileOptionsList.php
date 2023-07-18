<?php

declare(strict_types=1);

namespace Keboola\InputMapping\File\Options;

class InputFileOptionsList
{
    /**
     * @var InputFileOptions[]
     */
    private array $files = [];

    public function __construct(array $configurations, bool $isDevBranch, string $runId)
    {
        foreach ($configurations as $item) {
            $this->files[] = new InputFileOptions($item, $isDevBranch, $runId);
        }
    }

    /**
     * @returns InputFileOptions[]
     */
    public function getFiles(): array
    {
        return $this->files;
    }
}
