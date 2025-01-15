<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ClientFacadeFactory\Token;

readonly class StaticToken implements TokenInterface
{
    public function __construct(
        private string $value,
    ) {
    }

    public function getValue(): string
    {
        return $this->value;
    }
}
