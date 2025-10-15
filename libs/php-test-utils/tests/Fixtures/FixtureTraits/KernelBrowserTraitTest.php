<?php

declare(strict_types=1);

namespace Keboola\PhpTestUtils\Tests\Fixtures\FixtureTraits;

use Keboola\PhpTestUtils\Fixtures\FixtureTraits\KernelBrowserTrait;
use PHPUnit\Framework\TestCase;
use Symfony\Bundle\FrameworkBundle\KernelBrowser;

class KernelBrowserTraitTest extends TestCase
{
    private function createObject(): object
    {
        return new class {
            use KernelBrowserTrait;
        };
    }

    public function testSetAndGetBrowser(): void
    {
        $browser = $this->createStub(KernelBrowser::class);
        $obj = $this->createObject();

        self::assertTrue(method_exists($obj, 'setBrowser'));
        self::assertTrue(method_exists($obj, 'getBrowser'));

        $obj->setBrowser($browser);
        self::assertSame($browser, $obj->getBrowser());
    }
}
