<?php

namespace Keboola\InputMapping\File\Strategy;

use Keboola\InputMapping\Configuration\File\Manifest\Adapter as FileAdapter;
use Keboola\InputMapping\File\StrategyInterface;
use Symfony\Component\Filesystem\Filesystem;

class Local extends AbstractStrategy implements StrategyInterface
{
    public function downloadFile($fileInfo, $destinationPath)
    {
        if ($fileInfo['isSliced']) {
            $fs = new Filesystem();
            $fs->mkdir($this->ensurePathDelimiter($this->dataStorage->getPath()) . $destinationPath);
            $this->clientWrapper->getBasicClient()->downloadSlicedFile(
                $fileInfo['id'],
                $this->ensurePathDelimiter($this->dataStorage->getPath()) . $destinationPath
            );
        } else {
            $fs = new Filesystem();
            $fs->mkdir(dirname($this->ensurePathDelimiter($this->dataStorage->getPath()) . $destinationPath));
            $this->clientWrapper->getBasicClient()->downloadFile(
                $fileInfo['id'],
                $this->ensurePathDelimiter($this->dataStorage->getPath()) . $destinationPath
            );
        }

        $this->manifestWriter->writeFileManifest(
            $fileInfo,
            $this->ensurePathDelimiter($this->dataStorage->getPath()) . $destinationPath . '.manifest'
        );
        $manifest = $this->manifestCreator->createFileManifest($fileInfo);
        $adapter = new FileAdapter($this->format);
        $serializedManifest = $adapter->setConfig($manifest)->serialize();
        $manifestDestination = $this->ensurePathDelimiter($this->metadataStorage->getPath())
            . $destinationPath . '.manifest';
        $this->writeFile($serializedManifest, $manifestDestination);
    }

    public function writeFile($contents, $destination)
    {
        $fs = new Filesystem();
        $fs->dumpFile($destination, $contents);
    }
}
