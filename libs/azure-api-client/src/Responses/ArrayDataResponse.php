<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Responses;

use Keboola\AzureApiClient\ResponseModelInterface;

final class ArrayDataResponse implements ResponseModelInterface
{
    public function __construct(
        public readonly array $data,
    ) {
    }

    public static function fromResponseData(array $data): static
    {
        return new self($data);
    }
}
