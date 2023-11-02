<?php

declare(strict_types=1);

namespace Keboola\Slicer\Tests;

use Keboola\Slicer\Slicer;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Filesystem\Filesystem;

class SlicerTest extends TestCase
{
    public function testInstallSlicer(): void
    {
        Slicer::installSlicer();
        self::assertFileExists('bin/slicer');
        $fs = new Filesystem();
        $fs->remove('bin/slicer');
    }
}
