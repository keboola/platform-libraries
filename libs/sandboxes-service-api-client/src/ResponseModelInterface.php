<?php

declare(strict_types=1);

namespace Keboola\SandboxesServiceApiClient;

interface ResponseModelInterface
{
    public static function fromResponseData(array $data): static;
}
