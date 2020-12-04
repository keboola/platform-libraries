<?php


namespace Keboola\OutputMapping\Writer\Strategy\Files;


interface FilesStrategyInterface
{
    public function getManifestFiles($dir);

    /**
     * @param $dir
     * @return File[]
     */
    public function getFiles($dir);

    /**
     * @param string $file - fully qualified path to file
     * @param array $storageConfig
     * @return mixed
     */
    public function uploadFile($file, array $storageConfig = []);

    /**
     * @param string $manifestFile
     * @return array namifest
     */
    public function readFileManifest($manifestFile);
}
