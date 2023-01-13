<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Staging;

interface StagingInterface
{
    public static function getType(): string;
}
