<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Workspace;

use Keboola\KeyGenerator\PemKeyCertificateGenerator;
use Keboola\KeyGenerator\PemKeyCertificatePair;
use Keboola\StagingProvider\Workspace\SnowflakeKeypairGenerator;
use PHPUnit\Framework\TestCase;

class SnowflakeKeypairGeneratorTest extends TestCase
{
    public function testGenerateKeyPair(): void
    {
        $expectedKeyPair = new PemKeyCertificatePair(
            privateKey: 'private-key-content',
            publicKey: 'public-key-content',
        );

        $pemKeyCertificateGenerator = $this->createMock(PemKeyCertificateGenerator::class);
        $pemKeyCertificateGenerator
            ->expects(self::once())
            ->method('createPemKeyCertificate')
            ->with(null)
            ->willReturn($expectedKeyPair);

        $snowflakeKeypairGenerator = new SnowflakeKeypairGenerator($pemKeyCertificateGenerator);
        $actualKeyPair = $snowflakeKeypairGenerator->generateKeyPair();

        self::assertSame($expectedKeyPair, $actualKeyPair);
    }
}
