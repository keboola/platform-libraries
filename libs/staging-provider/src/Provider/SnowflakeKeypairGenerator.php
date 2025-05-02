<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Provider;

use Keboola\KeyGenerator\PemKeyCertificateGenerator;
use Keboola\KeyGenerator\PemKeyCertificatePair;

class SnowflakeKeypairGenerator
{
    public function __construct(
        private readonly PemKeyCertificateGenerator $pemKeyCertificateGenerator,
    ) {
    }

    public function generateKeyPair(): PemKeyCertificatePair
    {
        return $this->pemKeyCertificateGenerator->createPemKeyCertificate(null);
    }
}
