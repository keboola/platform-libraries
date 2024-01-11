<?php

declare(strict_types=1);

namespace Keboola\Slicer;

class Slicer
{
    public static function getBinaryPath(): string
    {
        return __DIR__ . '/../bin/slicer';
    }

    public static function installSlicer(): void
    {
        $downloader = new Downloader(
            new UrlResolver(),
            new MachineTypeResolver(
                php_uname('m'),
                PHP_OS,
            ),
            self::getBinaryPath(),
        );
        $downloader->download();
    }
}
