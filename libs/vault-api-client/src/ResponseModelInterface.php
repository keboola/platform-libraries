<?php

declare(strict_types=1);

namespace Keboola\VaultApiClient;

interface ResponseModelInterface
{
    public static function fromResponseData(array $data): static;
}
