<?php

namespace Keboola\InputMapping\Reader\Strategy\Files;

use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\InputMapping\Reader\WorkspaceProviderInterface;
use Keboola\StorageApi\Options\GetFileOptions;
use Keboola\StorageApi\Workspaces;
use Symfony\Component\Filesystem\Filesystem;

class ABSWorkspaceFilesStrategy extends AbstractFilesStrategy implements FilesStrategyInterface
{
    protected $workspaceType = WorkspaceProviderInterface::TYPE_ABS;

    protected $inputs = [];

    public function downloadFile($fileInfo, $destinationPath)
    {
        $this->inputs[] = [
            'dataFileId' => $fileInfo['id'],
            'destination' => $destinationPath,
        ];
        $this->manifestWriter->writeFileManifest($fileInfo, $destinationPath . ".manifest");
    }

    public function downloadFiles($fileConfigurations, $destination)
    {
        // Need to make the local destination dir to store manifests
        $fs = new Filesystem();
        $fs->mkdir($destination);

        parent::downloadFiles($fileConfigurations, $destination);
        if (!empty($this->inputs)) {
            $workspaces = new Workspaces($this->clientWrapper->getBasicClient());
            $workspaceId = $this->workspaceProvider->getWorkspaceId($this->workspaceType);
            $workspaces->loadWorkspaceData($workspaceId, [
                'input' => $this->inputs,
            ]);
            $this->logger->info('All files were fetched.');
        }
    }
}
