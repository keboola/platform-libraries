<?php

declare(strict_types=1);

namespace Keboola\VaultApiClient\Tests;

use Keboola\VaultApiClient\ResponseModelInterface;

final class DummyTestResponse implements ResponseModelInterface
{
    public function __construct(
        public readonly string $foo,
    ) {
    }

    public static function fromResponseData(array $data): static
    {
        return new self(
            $data['foo'],
        );
    }
}
