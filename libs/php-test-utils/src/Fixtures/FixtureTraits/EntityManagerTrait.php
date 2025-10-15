<?php

declare(strict_types=1);

namespace Keboola\PhpTestUtils\Fixtures\FixtureTraits;

use Doctrine\ORM\EntityManagerInterface;

trait EntityManagerTrait
{
    private EntityManagerInterface $em;

    public function setEntityManager(EntityManagerInterface $em): void
    {
        $this->em = $em;
    }

    public function getEntityManager(): EntityManagerInterface
    {
        return $this->em;
    }
}
