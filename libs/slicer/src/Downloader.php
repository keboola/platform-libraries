<?php

declare(strict_types=1);

namespace Keboola\Slicer;

use RuntimeException;
use Symfony\Component\Filesystem\Filesystem;

class Downloader
{
    private Filesystem $fs;

    public function __construct(
        private readonly UrlResolver $urlResolver,
        private readonly MachineTypeResolver $machineTypeResolver,
        private readonly string $targetDir,
    ) {
        $this->fs = new Filesystem();
    }

    public function download(): string
    {
        $url = $this->urlResolver->getDownloadUrl(
            $this->machineTypeResolver->getOperatingSystemName(),
            $this->machineTypeResolver->getPlatformName(),
            $this->machineTypeResolver->getSuffix(),
        );
        return $this->doDownload($url, $this->targetDir . '/slicer');
    }

    private function doDownload(string $url, string $targetFile): string
    {
        $curl = curl_init($url);
        if ($curl === false) {
            throw new RuntimeException(sprintf('Cannot open curl "%s".', $url));
        }

        $fp = fopen($targetFile, 'w+');
        if ($fp === false) {
            throw new RuntimeException(sprintf('Cannot open file "%s".', $targetFile));
        }

        curl_setopt($curl, CURLOPT_FOLLOWLOCATION, 1);
        curl_setopt($curl, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($curl, CURLOPT_FAILONERROR, true);
        curl_setopt($curl, CURLOPT_FILE, $fp);

        if (curl_exec($curl) === false) {
            throw new RuntimeException(
                sprintf(
                    'File "%s" download failed: %s - %s',
                    $url,
                    curl_errno($curl),
                    curl_error($curl),
                ),
            );
        }
        curl_close($curl);
        $this->fs->chmod($targetFile, 0777);
        return $targetFile;
    }
}
