<?php

declare(strict_types=1);

namespace Keboola\PhpTestUtils\Fixtures\FixtureTraits;

use Doctrine\ORM\EntityManagerInterface;

/**
 * Marker interface to express presence of EntityManagerTrait methods in PHPDoc/static analysis.
 */
interface EntityManagerAware
{
    public function setEntityManager(EntityManagerInterface $em): void;

    public function getEntityManager(): EntityManagerInterface;
}
