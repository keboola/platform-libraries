<?php

namespace Keboola\OutputMapping\Writer\Helper;

use Symfony\Component\Finder\Finder;
use Symfony\Component\Finder\SplFileInfo;

class ManifestHelper
{
    public static function getManifestFiles($dir)
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
}
