<?php

declare(strict_types=1);

namespace Keboola\Slicer\Tests;

use Keboola\Slicer\UrlResolver;
use PHPUnit\Framework\TestCase;
use ReflectionProperty;

class UrlResolverTest extends TestCase
{
    public function testGetDownloadUrl(): void
    {
        $urlResolver = new UrlResolver();
        self::assertSame(
            'https://github.com/keboola/processor-split-table/releases/download/v2.0.0/cli_linux_amd64',
            $urlResolver->getDownloadUrl('linux', 'amd64', ''),
        );
    }

    public function testGetDownloadUrlPagination(): void
    {
        $urlResolver = new UrlResolver();
        $reflection = new ReflectionProperty(UrlResolver::class, 'pageSize');
        $reflection->setAccessible(true);
        $reflection->setValue($urlResolver, 1);

        $reflection = new ReflectionProperty(UrlResolver::class, 'slicerBaseVersion');
        $reflection->setAccessible(true);
        $reflection->setValue($urlResolver, 'v1.3');

        self::assertSame(
            'https://github.com/keboola/processor-split-table/releases/download/v1.3.1/cli_win_arm64.exe',
            $urlResolver->getDownloadUrl('win', 'arm64', '.exe'),
        );
    }
}
