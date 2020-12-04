<?php

namespace Keboola\OutputMapping\Writer\Strategy\Files;

use Keboola\OutputMapping\Configuration\File\Manifest\ABSWorkspaceFileAdapter;
use Keboola\OutputMapping\Exception\InvalidOutputException;

class AbsWorkspaceFilesStrategy extends AbstractFilesStrategy implements FilesStrategyInterface
{
    public function getManifestFiles($dir)
    {
        $finder = new Finder();
        $manifests = $finder->files()->name('*.manifest')->in($dir)->depth(0);
        $manifestFileNames = [];
        /** @var SplFileInfo $manifest */
        foreach ($manifests as $manifest) {
            $manifestFileNames[] = $manifest->getPathname();
        }
        return $manifestFileNames;
    }

    public function readFileManifest($manifestFile)
    {
        $adapter = new ABSWorkspaceFileAdapter($this->format);
        try {
            return $adapter->readFromFile($manifestFile);
        } catch (\Exception $e) {
            throw new InvalidOutputException(
                sprintf(
                    'Failed to parse manifest file "%s" as "%s": %s',
                    $manifestFile,
                    $this->format,
                    $e->getMessage()
                ),
                $e->getCode(),
                $e
            );
        }
    }

    /**
     * @inheritDoc
     */
    public function getFiles($dir)
    {
        // TODO: Implement getFiles() method.
    }

    /**
     * @inheritDoc
     */
    public function uploadFile($file, array $storageConfig = [])
    {
        // TODO: Implement uploadFile() method.
    }
}
