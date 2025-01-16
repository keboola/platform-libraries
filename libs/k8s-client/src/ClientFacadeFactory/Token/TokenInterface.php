<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ClientFacadeFactory\Token;

interface TokenInterface
{
    public function getValue(): string;
}
