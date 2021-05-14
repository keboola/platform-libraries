<?php

namespace Keboola\InputMapping\File\Strategy;

use Keboola\InputMapping\Configuration\File\Manifest\Adapter as FileAdapter;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\File\StrategyInterface;
use Symfony\Component\Filesystem\Filesystem;

class Local extends AbstractStrategy implements StrategyInterface
{
    public function downloadFile($fileInfo, $destinationPath, $overwrite)
    {
        if ($overwrite === false) {
            throw new InvalidInputException('Overwrite cannot be turned off for local mapping.');
        }
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
        $manifest = $this->manifestCreator->createFileManifest($fileInfo);
        $adapter = new FileAdapter($this->format);
        $serializedManifest = $adapter->setConfig($manifest)->serialize();
        $manifestDestination = $this->ensurePathDelimiter($this->metadataStorage->getPath())
            . $destinationPath . '.manifest';
        $this->writeFile($serializedManifest, $manifestDestination);
    }

    private function writeFile($contents, $destination)
    {
        $fs = new Filesystem();
        $fs->dumpFile($destination, $contents);
    }

    protected function getFileDestinationPath($destinationPath, $fileId, $fileName)
    {
        /* this is the actual file name being used by the export, hence it contains file id + file name */
        return sprintf(
            '%s/%s_%s',
            $this->ensureNoPathDelimiter($destinationPath),
            $fileId,
            $fileName
        );
    }
}
