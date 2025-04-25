<?php

declare(strict_types=1);

namespace Keboola\KeyGenerator;

use SensitiveParameter;

class PemKeyCertificatePair
{
    public function __construct(
        #[SensitiveParameter] private readonly string $privateKey,
        private readonly string $publicKey,
    ) {
    }

    public function getPrivateKey(): string
    {
        return $this->privateKey;
    }

    public function getPublicKey(): string
    {
        return $this->publicKey;
    }
}
