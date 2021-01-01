<?php

namespace Keboola\InputMapping\File\Strategy;

use Keboola\InputMapping\File\StrategyInterface;
use Keboola\StorageApi\Workspaces;

class ABSWorkspace extends AbstractStrategy implements StrategyInterface
{
    protected $inputs = [];

    public function downloadFile($fileInfo, $destinationPath)
    {
        $this->inputs[] = [
            'dataFileId' => $fileInfo['id'],
            'destination' => $destinationPath,
        ];
        $this->manifestWriter->writeFileManifest(
            $fileInfo,
            $this->ensurePathDelimiter($this->metadataStorage->getPath()) . $destinationPath . '.manifest'
        );
    }

    public function downloadFiles($fileConfigurations, $destination)
    {
        parent::downloadFiles($fileConfigurations, $destination);
        if (!empty($this->inputs)) {
            $workspaces = new Workspaces($this->clientWrapper->getBasicClient());
            $workspaceId = $this->dataStorage->getWorkspaceId();
            $workspaces->loadWorkspaceData($workspaceId, [
                'input' => $this->inputs,
                'preserve' => 1,
            ]);
            $this->logger->info('All files were fetched.');
        }
    }
}
