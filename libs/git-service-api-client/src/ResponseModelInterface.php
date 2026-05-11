<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient;

interface ResponseModelInterface
{
    /**
     * @param array<string, mixed> $data
     */
    public static function fromResponseData(array $data): static;
}
