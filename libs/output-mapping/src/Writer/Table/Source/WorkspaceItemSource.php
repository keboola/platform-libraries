<?php

namespace Keboola\OutputMapping\Writer\Table\Source;

use InvalidArgumentException;

class WorkspaceItemSource implements SourceInterface
{
    /** @var string */
    private $sourceName;

    /** @var string */
    private $workspaceId;

    /** @var string */
    private $dataObject;

    /** @var bool */
    private $isSliced;

    /**
     * @param string $sourceName
     * @param string $workspaceId
     * @param string $dataObject
     * @param bool $isSliced
     */
    public function __construct($sourceName, $workspaceId, $dataObject, $isSliced)
    {
        if (!is_string($sourceName)) {
            throw new InvalidArgumentException(sprintf(
                'Argument $sourceName must be a string, %s given',
                is_object($sourceName) ? get_class($sourceName) : gettype($sourceName)
            ));
        }

        if (!is_string($workspaceId)) {
            throw new InvalidArgumentException(sprintf(
                'Argument $workspaceId must be a string, %s given',
                is_object($workspaceId) ? get_class($workspaceId) : gettype($workspaceId)
            ));
        }

        if (!is_string($dataObject)) {
            throw new InvalidArgumentException(sprintf(
                'Argument $dataObject must be a string, %s given',
                is_object($dataObject) ? get_class($dataObject) : gettype($dataObject)
            ));
        }

        if (!is_bool($isSliced)) {
            throw new InvalidArgumentException(sprintf(
                'Argument $isSliced must be a boolean, %s given',
                is_object($isSliced) ? get_class($isSliced) : gettype($isSliced)
            ));
        }

        $this->sourceName = $sourceName;
        $this->workspaceId = $workspaceId;
        $this->dataObject = $dataObject;
        $this->isSliced = $isSliced;
    }

    public function getName()
    {
        return $this->sourceName;
    }

    public function getWorkspaceId()
    {
        return $this->workspaceId;
    }

    public function getDataObject()
    {
        return $this->dataObject;
    }

    public function isSliced()
    {
        return $this->isSliced;
    }
}
