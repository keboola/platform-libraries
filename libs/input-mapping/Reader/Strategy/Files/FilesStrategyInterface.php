<?php

namespace Keboola\InputMapping\Reader\Strategy\Files;

interface FilesStrategyInterface
{
    public function downloadFile($fileInfo, $destinationPath);
}
