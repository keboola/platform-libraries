<?php

declare(strict_types=1);

namespace Keboola\Slicer;

use Composer\Script\Event;

class Slicer
{
    public static function installSlicer(
        Event $event,
    ): void {
        $binDir = $event->getComposer()->getConfig()->get('bin-dir');
        assert(is_string($binDir));
        $downloader = new Downloader(
            new UrlResolver(),
            new MachineTypeResolver(
                php_uname('m'),
                PHP_OS,
            ),
            $binDir,
        );
        $downloader->download();
    }
}
