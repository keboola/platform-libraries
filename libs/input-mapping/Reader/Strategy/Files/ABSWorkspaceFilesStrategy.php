<?php

namespace Keboola\InputMapping\Reader\Strategy\Files;

use Keboola\InputMapping\Reader\WorkspaceProviderInterface;
use Keboola\StorageApi\Workspaces;

class ABSWorkspaceFilesStrategy extends AbstractFilesStrategy implements FilesStrategyInterface
{
    protected $workspaceType = WorkspaceProviderInterface::TYPE_ABS;

    public function downloadFile($fileInfo, $destinationPath)
    {
        $workspaces = new Workspaces($this->clientWrapper->getBasicClient());
        $workspaceId = $this->workspaceProvider->getWorkspaceId($this->workspaceType);
        $workspaces->loadWorkspaceData($workspaceId, [
            'dataFileId' => $fileInfo['id'],
            'destination' => $destinationPath,
        ]);
        $this->manifestWriter->writeFileManifest($fileInfo, $destinationPath . ".manifest");
    }
}
