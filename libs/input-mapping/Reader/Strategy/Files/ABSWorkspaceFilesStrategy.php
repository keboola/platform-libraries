<?php

namespace Keboola\InputMapping\Reader\Strategy\Files;

use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\InputMapping\Reader\WorkspaceProviderInterface;
use Keboola\StorageApi\Options\GetFileOptions;
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
    }

    public function downloadFiles($fileConfigurations, $destination)
    {
        $fileOptions = new GetFileOptions();
        $fileOptions->setFederationToken(true);
        $inputs = [];
        foreach ($fileConfigurations as $fileConfiguration) {
            $files = $this->getFiles($fileConfiguration);
            foreach ($files as $file) {
                $fileInfo = $this->clientWrapper->getBasicClient()->getFile($file['id'], $fileOptions);
                $fileDestinationPath = sprintf('%s/%s_%s', $destination, $fileInfo['id'], $fileInfo["name"]);
                $this->logger->info(sprintf('Fetching file %s (%s).', $fileInfo['name'], $file['id']));
                $inputs[] = [
                    'dataFileId' => $fileInfo['id'],
                    'destination' => $fileDestinationPath,
                ];
                try {
                    // add leading slash because we write the manifests locally
                    $this->manifestWriter->writeFileManifest($fileInfo, '/' . $fileDestinationPath . ".manifest");
                } catch (\Exception $e) {
                    throw new InputOperationException(
                        sprintf('Failed to write manifest file %s (%s).', $fileInfo['name'], $file['id']),
                        0,
                        $e
                    );
                }
                $this->logger->info(sprintf('Prepared file %s (%s).', $fileInfo['name'], $file['id']));
            }
        }
        if (!empty($inputs)) {
            $workspaces = new Workspaces($this->clientWrapper->getBasicClient());
            $workspaceId = $this->workspaceProvider->getWorkspaceId($this->workspaceType);
            $workspaces->loadWorkspaceData($workspaceId, [
                'input' => $inputs,
            ]);
            $this->logger->info('All files were fetched.');
        }
    }
}
