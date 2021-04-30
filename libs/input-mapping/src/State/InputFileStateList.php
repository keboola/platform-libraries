<?php

namespace Keboola\InputMapping\State;

use JsonSerializable;
use Keboola\InputMapping\Exception\FileNotFoundException;

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

    /**
     * @param $fileTags
     * @return InputFileState
     * @throws FileNotFoundException
     */
    public function getFile($fileTags)
    {
        foreach ($this->files as $file) {
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
