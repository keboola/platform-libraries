<?php

namespace Keboola\InputMapping\State;

use JsonSerializable;
use Keboola\InputMapping\Exception\FileNotFoundException;
use Keboola\InputMapping\Helper\BuildQueryFromConfigurationHelper;

class InputFileStateList implements JsonSerializable
{
    /**
     * @var InputFileState[]
     */
    private $files = [];

    public function __construct(array $configurations)
    {
        foreach ($configurations as $item) {
            $this->files[] = new InputFileState($item);
        }
    }

    public function getFileConfigurationIdentifier(array $fileConfiguration)
    {
        return (isset($fileConfiguration['tags']))
            ? BuildQueryFromConfigurationHelper::getSourceTagsFromTags($fileConfiguration['tags'])
            : (isset($fileConfiguration['source']['tags']) ? $fileConfiguration['source']['tags'] : []);
    }

    /**
     * @param $fileTags
     * @return InputFileState
     * @throws FileNotFoundException
     */
    public function getFile($fileTags)
    {
        foreach ($this->files as $file) {
            var_dump($file->getTags());
            if ($file->getTags() === $fileTags) {
                return $file;
            }
        }
        throw new FileNotFoundException('State for files defined by "' . json_encode($fileTags) . '" not found.');
    }

    /**
     * @return array
     */
    public function jsonSerialize()
    {
        return array_map(function (InputFileState $file) {
            return $file->jsonSerialize();
        }, $this->files);
    }
}
