<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Tests\Fixtures;

use Keboola\ApiClientBase\ResponseModelInterface;

final class DummyModel implements ResponseModelInterface
{
    public function __construct(public readonly string $name)
    {
    }

    public static function fromResponseData(array $data): static
    {
        \assert(is_string($data['name']));
        return new self($data['name']);
    }
}
