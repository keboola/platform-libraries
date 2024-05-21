<?php

declare(strict_types=1);

namespace Keboola\Slicer;

use JsonException;
use RuntimeException;

class UrlResolver
{
    // phpcs:ignore Generic.Files.LineLength.MaxExceeded
    private const SLICER_URL_TEMPLATE = 'https://github.com/keboola/processor-split-table/releases/download/%s/cli_%s_%s%s';
    // phpcs:ignore Generic.Files.LineLength.MaxExceeded
    private const SLICER_VERSION_URL_TEMPLATE = 'https://api.github.com/repos/keboola/processor-split-table/releases?perPage=%s&page=%s';

    private int $pageSize;
    private string $slicerBaseVersion;

    public function __construct()
    {
        $this->pageSize = 100;
        $this->slicerBaseVersion = 'v3.0';
    }

    public function getDownloadUrl(
        string $operatingSystem,
        string $platform,
        string $fileExtension,
    ): string {
        return sprintf(
            self::SLICER_URL_TEMPLATE,
            $this->getSlicerVersion(),
            $operatingSystem,
            $platform,
            $fileExtension,
        );
    }

    private function getReleasesListPage(int $page): string
    {
        $url = sprintf(self::SLICER_VERSION_URL_TEMPLATE, $this->pageSize, $page);
        $curl = curl_init($url);
        if ($curl === false) {
            throw new RuntimeException(sprintf('Cannot open curl "%s".', $url));
        }
        curl_setopt($curl, CURLOPT_FOLLOWLOCATION, 1);
        curl_setopt($curl, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($curl, CURLOPT_FAILONERROR, true);
        curl_setopt($curl, CURLOPT_USERAGENT, 'Slicer tool installer');
        $response = curl_exec($curl);
        if ($response === false) {
            throw new RuntimeException(
                sprintf(
                    'Cannot list versions from "%s": %s - %s',
                    $url,
                    curl_errno($curl),
                    curl_error($curl),
                ),
            );
        }
        curl_close($curl);
        return (string) $response;
    }

    private function getSlicerVersion(): string
    {
        $page = 1;
        $tagNames = [];
        do {
            $response = $this->getReleasesListPage($page);
            try {
                $result = (array) json_decode($response, true, flags: JSON_THROW_ON_ERROR);
            } catch (JsonException $e) {
                throw new RuntimeException(
                    sprintf('Cannot parse response "%s": %s', $response, $e->getMessage()),
                );
            }
            foreach ($result as $release) {
                /** @var array $release */
                $tagNames[] = $release['tag_name'];
            }
            $page++;
        } while ($result);
        rsort($tagNames);
        foreach ($tagNames as $tagName) {
            if (str_starts_with($tagName, $this->slicerBaseVersion)) {
                return $tagName;
            }
        }
        throw new RuntimeException(
            sprintf('Cannot find slicer version starting with "%s".', $this->slicerBaseVersion),
        );
    }
}
