<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\Strategy;

use InvalidArgumentException;
use Keboola\OutputMapping\Writer\Table\Source\SourceInterface;
use Keboola\OutputMapping\Writer\Table\Source\WorkspaceItemSource;

abstract class AbstractWorkspaceTableStrategy extends AbstractTableStrategy
{
    /**
     * @return array {
     *      dataWorkspaceId: string,
     *      dataObject: string
     * }
     */
    public function prepareLoadTaskOptions(SourceInterface $source, array $config): array
    {
        if (!$source instanceof WorkspaceItemSource) {
            throw new InvalidArgumentException(sprintf(
                'Argument $source is expected to be instance of %s, %s given',
                WorkspaceItemSource::class,
                get_class($source),
            ));
        }

        return [
            'dataWorkspaceId' => (string) $source->getWorkspaceId(),
            'dataObject' => (string) $source->getDataObject(),
        ];
    }
}
