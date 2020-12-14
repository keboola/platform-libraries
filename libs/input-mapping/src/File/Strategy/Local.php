<?php

namespace Keboola\InputMapping\File\Strategy;

use Keboola\InputMapping\File\StrategyInterface;
use Symfony\Component\Filesystem\Filesystem;

class Local extends AbstractStrategy implements StrategyInterface
{
    public function downloadFiles($fileConfigurations, $destination)
    {
        parent::downloadFiles($fileConfigurations, $destination);
    }

    public function downloadFile($fileInfo, $destinationPath)
    {
        if ($fileInfo['isSliced']) {
            $fs = new Filesystem();
            $fs->mkdir($this->dataStorage->getPath() . $destinationPath);
            $this->clientWrapper->getBasicClient()->downloadSlicedFile(
                $fileInfo['id'],
                $this->dataStorage->getPath() . $destinationPath
            );
        } else {
            $this->clientWrapper->getBasicClient()->downloadFile(
                $fileInfo['id'],
                $this->dataStorage->getPath() . $destinationPath
            );
        }
        $this->manifestWriter->writeFileManifest(
            $fileInfo,
            $this->dataStorage->getPath() . $destinationPath . '.manifest'
        );
    }
}
