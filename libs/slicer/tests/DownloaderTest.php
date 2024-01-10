<?php

declare(strict_types=1);

namespace Keboola\Slicer\Tests;

use Keboola\Slicer\Downloader;
use Keboola\Slicer\MachineTypeResolver;
use Keboola\Slicer\UrlResolver;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Filesystem\Filesystem;

class DownloaderTest extends TestCase
{
    public const BIN_DIRECTORY_PATH = __DIR__ . '/../vendor/bin';

    public function testDownload(): void
    {
        chdir(sys_get_temp_dir());
        $fs = new Filesystem();
        $fs->remove(self::BIN_DIRECTORY_PATH . '/slicer');
        $downloader = new Downloader(
            new UrlResolver(),
            new MachineTypeResolver(
                php_uname('m'),
                PHP_OS,
            ),
            self::BIN_DIRECTORY_PATH,
        );

        $result = $downloader->download();
        self::assertFileExists($result);
        self::assertGreaterThan(100000, filesize($result), (string) file_get_contents($result));
        self::assertMatchesRegularExpression('/bin\/slicer/', $result);
    }

    public function testDownloadFailed(): void
    {
        chdir(sys_get_temp_dir());
        $fs = new Filesystem();
        $fs->remove(self::BIN_DIRECTORY_PATH . '/slicer');

        $downloader = new Downloader(
            new UrlResolver(),
            new MachineTypeResolver(
                '64',
                'Owen',
            ),
            self::BIN_DIRECTORY_PATH,
        );

        $result = $downloader->download();
        self::assertFileExists($result);
        self::assertGreaterThan(100000, filesize($result), (string) file_get_contents($result));
        self::assertMatchesRegularExpression('/bin\/slicer/', $result);
    }
}
