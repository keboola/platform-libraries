<?php

namespace Keboola\InputMapping\File;

interface StrategyInterface
{
    public function downloadFile($fileInfo, $destinationPath);

    public function writeFile($contents, $destination);
}
