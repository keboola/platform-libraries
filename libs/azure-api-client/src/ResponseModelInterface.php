<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient;

interface ResponseModelInterface
{
    public static function fromResponseData(array $data): static;
}
