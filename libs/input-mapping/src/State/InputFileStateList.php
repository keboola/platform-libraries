<?php

declare(strict_types=1);

namespace Keboola\InputMapping\State;

use JsonSerializable;
use Keboola\InputMapping\Exception\FileNotFoundException;
use Keboola\InputMapping\Helper\BuildQueryFromConfigurationHelper;

class InputFileStateList implements JsonSerializable
{
    /**
     * @var InputFileState[]
     */
    private array $files = [];

    public function __construct(array $configurations)
    {
        foreach ($configurations as $item) {
            $this->files[] = new InputFileState($item);
        }
    }

    public function getFileConfigurationIdentifier(array $fileConfiguration): array
    {
        return (isset($fileConfiguration['tags']))
            ? BuildQueryFromConfigurationHelper::getSourceTagsFromTags($fileConfiguration['tags'])
            : ($fileConfiguration['source']['tags'] ?? []);
    }

    /**
     * @throws FileNotFoundException
     */
    public function getFile(array $fileTags): InputFileState
    {
        foreach ($this->files as $file) {
            if ($file->getTags() === $fileTags) {
                return $file;
            }
        }
        throw new FileNotFoundException('State for files defined by "' . json_encode($fileTags) . '" not found.');
    }

    public function jsonSerialize(): array
    {
        return array_map(function (InputFileState $file) {
            return $file->jsonSerialize();
        }, $this->files);
    }
}
