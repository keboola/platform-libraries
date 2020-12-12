<?php

namespace Keboola\InputMapping\File\Strategy;

use Keboola\InputMapping\File\StrategyInterface;
use Keboola\InputMapping\WorkspaceProviderInterface;
use Keboola\StorageApi\Workspaces;
use Symfony\Component\Filesystem\Filesystem;

class ABSWorkspace extends AbstractStrategy implements StrategyInterface
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
