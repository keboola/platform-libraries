<?php

declare(strict_types=1);

namespace Keboola\PhpTestUtils\Tests\Fixtures\FixtureTraits;

use Doctrine\ORM\EntityManagerInterface;
use Keboola\PhpTestUtils\Fixtures\FixtureTraits\EntityManagerTrait;
use PHPUnit\Framework\TestCase;

class EntityManagerTraitTest extends TestCase
{
    private function createObject(): object
    {
        return new class {
            use EntityManagerTrait;
        };
    }

    public function testSetAndGetEntityManager(): void
    {
        $em = $this->createStub(EntityManagerInterface::class);
        $obj = $this->createObject();

        self::assertTrue(method_exists($obj, 'getEntityManager'));
        self::assertTrue(method_exists($obj, 'setEntityManager'));

        $obj->setEntityManager($em);
        self::assertSame($em, $obj->getEntityManager());
    }
}
