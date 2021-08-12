<?php

namespace Keboola\OutputMapping\Writer\Helper;

use Symfony\Component\Finder\Finder;
use Symfony\Component\Finder\SplFileInfo;

class ManifestHelper
{
    /**
     * @param string $dir
     * @return array<SplFileInfo>
     */
    public static function getManifestFiles($dir)
    {
        $files = (new Finder())->files()->name('*.manifest')->in($dir)->depth(0);
        return iterator_to_array($files);
    }

    /**
     * @param string $dir
     * @return array<SplFileInfo>
     */
    public static function getNonManifestFiles($dir)
    {
        $files = (new Finder())->notName('*.manifest')->in($dir)->depth(0);
        return iterator_to_array($files);
    }
}
