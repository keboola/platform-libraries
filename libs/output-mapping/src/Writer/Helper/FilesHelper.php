<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Helper;

use Symfony\Component\Finder\Finder;
use Symfony\Component\Finder\SplFileInfo;

class FilesHelper
{
    /**
     * Returns list of manifest files in the directory.
     *
     * @return array<SplFileInfo>
     */
    public static function getManifestFiles(string $dir): array
    {
        $files = (new Finder())->files()->name('*.manifest')->in($dir)->depth(0);
        return iterator_to_array($files);
    }

    /**
     * Returns list of data files in the directory.
     *
     * Data files may not be only regular files, but also directories, representing sliced files.
     *
     * @return array<SplFileInfo>
     */
    public static function getDataFiles(string $dir): array
    {
        $files = (new Finder())->notName('*.manifest')->in($dir)->depth(0);
        return iterator_to_array($files);
    }
}
