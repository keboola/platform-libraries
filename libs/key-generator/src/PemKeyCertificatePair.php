<?php

declare(strict_types=1);

namespace Keboola\KeyGenerator;

use SensitiveParameter;

readonly class PemKeyCertificatePair
{
    public function __construct(
        #[SensitiveParameter] public string $privateKey,
        public string $publicKey,
    ) {
    }
}
