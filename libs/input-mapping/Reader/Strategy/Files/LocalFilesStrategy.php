<?php

namespace Keboola\InputMapping\Reader\Strategy\Files;

use Symfony\Component\Filesystem\Filesystem;

class LocalFilesStrategy extends AbstractFilesStrategy implements FilesStrategyInterface
{
    public function downloadFiles($fileConfigurations, $destination)
    {
        $fs = new Filesystem();
        $fs->mkdir($destination);
        parent::downloadFiles($fileConfigurations, $destination);
    }

    public function downloadFile($fileInfo, $destinationPath)
    {
        if ($fileInfo['isSliced']) {
            $this->clientWrapper->getBasicClient()->downloadSlicedFile($fileInfo['id'], $destinationPath);
        } else {
            $this->clientWrapper->getBasicClient()->downloadFile($fileInfo['id'], $destinationPath);
        }
        $this->manifestWriter->writeFileManifest($fileInfo, $destinationPath . ".manifest");
    }
}
