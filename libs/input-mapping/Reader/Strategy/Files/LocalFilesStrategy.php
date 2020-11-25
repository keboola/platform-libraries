<?php

namespace Keboola\InputMapping\Reader\Strategy\Files;

class LocalFilesStrategy extends AbstractFilesStrategy implements FilesStrategyInterface
{
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
