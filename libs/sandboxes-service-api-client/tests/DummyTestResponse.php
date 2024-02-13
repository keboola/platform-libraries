<?php

declare(strict_types=1);

namespace Keboola\SandboxesServiceApiClient\Tests;

use Keboola\SandboxesServiceApiClient\ResponseModelInterface;

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
