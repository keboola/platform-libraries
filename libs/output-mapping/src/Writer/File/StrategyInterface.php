<?php

namespace Keboola\OutputMapping\Writer\File;

interface StrategyInterface
{
    /**
     * @param string $dir
     * @return FileItem[]
     */
    public function listFiles($dir);

    /**
     * @param string $dir
     * @return FileItem[] Indexed by file path.
     */
    public function listManifests($dir);

    /**
     * @param string $file - fully qualified path to file
     * @param array $storageConfig
     * @return string Storage File Id
     */
    public function loadFileToStorage($file, array $storageConfig);

    /**
     * @param string $manifestFile
     * @return array Manifest data
     */
    public function readFileManifest($manifestFile);
}
