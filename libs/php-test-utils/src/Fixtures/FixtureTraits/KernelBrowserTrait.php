<?php

declare(strict_types=1);

namespace Keboola\PhpTestUtils\Fixtures\FixtureTraits;

use Symfony\Bundle\FrameworkBundle\KernelBrowser;

trait KernelBrowserTrait
{
    private KernelBrowser $browser;

    public function setBrowser(KernelBrowser $browser): void
    {
        $this->browser = $browser;
    }

    public function getBrowser(): KernelBrowser
    {
        return $this->browser;
    }
}
