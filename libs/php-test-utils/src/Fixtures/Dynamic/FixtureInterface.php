<?php

declare(strict_types=1);

namespace Keboola\PhpTestUtils\Fixtures\Dynamic;

interface FixtureInterface
{
    public function initialize(): void;
    public function cleanUp(): void;
}
