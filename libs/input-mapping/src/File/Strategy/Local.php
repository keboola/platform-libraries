<?php

declare(strict_types=1);

namespace Keboola\InputMapping\File\Strategy;

use Keboola\InputMapping\Configuration\File\Manifest\Adapter as FileAdapter;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\File\StrategyInterface;
use Keboola\StorageApi\Client;
use Symfony\Component\Filesystem\Filesystem;

class Local extends AbstractStrategy implements StrategyInterface
{
    public function downloadFile(array $fileInfo, string $destinationPath, bool $overwrite, Client $client): void
    {
        if ($overwrite === false) {
            throw new InvalidInputException('Overwrite cannot be turned off for local mapping.');
        }
        $fs = new Filesystem();
        if ($fileInfo['isSliced']) {
            $fs->mkdir($this->ensurePathDelimiter($this->dataStorage->getPath()) . $destinationPath);
            $client->downloadSlicedFile(
                $fileInfo['id'],
                $this->ensurePathDelimiter($this->dataStorage->getPath()) . $destinationPath
            );
        } else {
            $fs->mkdir(dirname($this->ensurePathDelimiter($this->dataStorage->getPath()) . $destinationPath));
            $client->downloadFile(
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

    private function writeFile(string $contents, string $destination): void
    {
        $fs = new Filesystem();
        $fs->dumpFile($destination, $contents);
    }

    protected function getFileDestinationPath(string $destinationPath, int $fileId, string $fileName): string
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
