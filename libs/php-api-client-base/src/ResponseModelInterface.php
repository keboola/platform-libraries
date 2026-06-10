<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase;

interface ResponseModelInterface
{
    /**
     * @param array<mixed> $data
     */
    public static function fromResponseData(array $data): static;
}
