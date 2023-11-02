<?php

declare(strict_types=1);

namespace Keboola\Slicer;

use Exception;
use RuntimeException;

class Slicer
{
    private const SLICER_VERSION = '2.0.0';
    public static function installSlicer(): void
    {
        $uname = strtoupper(php_uname('m'));
        var_dump($uname);
        if (str_contains(PHP_OS, 'WIN')) {
            $os = 'win';
            $suffix = '.exe';
        } elseif (str_contains(PHP_OS, 'DARWIN')) {
            $os = 'macos';
            $suffix = '';
        } else {
            $os = 'linux';
            $suffix = '';
        }

        if (str_contains($uname, 'AMD64')) {
            $platform = 'amd64';
        } elseif (str_contains($uname, 'ARM64')) {
            $platform = 'arm64';
        } else {
            throw new RuntimeException(sprintf('Unsupported platform "%s".', $uname));
        }

        var_dump($os);
        var_dump($platform);
        $pwd = getcwd();
        var_dump($pwd);
        $pwd .= '/bin';
        var_dump($pwd);
        if (!file_exists($pwd)) {
            mkdir($pwd);
        }
        $pwd .= '/slicer.exe';

        $url = sprintf(
            'https://github.com/keboola/processor-split-table/releases/download/v%s/cli_%s_%s%s',
            self::SLICER_VERSION,
            $os,
            $platform,
            $suffix,
        );

        $command = sprintf('curl -fL -o %s %s',
            $pwd,
            $url
        );
        var_dump($command);
        exec($command, $output, $returnCode);
        var_dump($output);
        var_dump($returnCode);

        if ($returnCode != 0) {
            throw new Exception('Cooties!');
        }
    }
}
