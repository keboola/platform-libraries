<?php

declare(strict_types=1);

namespace Keboola\Slicer;

class Slicer
{
    public static function installSlicer(): void
    {
        $downloader = new Downloader(
            new UrlResolver(),
            new MachineTypeResolver(
                php_uname('m'),
                PHP_OS,
            ),
        );
        $downloader->download();
    }
}
