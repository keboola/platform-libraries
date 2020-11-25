<?php


namespace Keboola\InputMapping\Reader\Strategy\Files;

interface FilesStrategyInterface
{
    public function downloadFiles($configuration);
    public function downloadFile($fileInfo, $destinationPath);
}
