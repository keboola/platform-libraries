<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\Source;

interface SourceInterface
{
    public function getName(): string;

    public function isSliced(): bool;
}
