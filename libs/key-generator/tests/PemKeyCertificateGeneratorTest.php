<?php

declare(strict_types=1);

namespace Keboola\KeyGenerator\Tests;

use Keboola\KeyGenerator\PemKeyCertificateGenerator;
use PHPUnit\Framework\TestCase;

class PemKeyCertificateGeneratorTest extends TestCase
{
    private PemKeyCertificateGenerator $generator;

    protected function setUp(): void
    {
        parent::setUp();
        $this->generator = new PemKeyCertificateGenerator();
    }

    public function testCreatePemKeyCertificateWithoutPassword(): void
    {
        $pair = $this->generator->createPemKeyCertificate(null);

        $this->assertStringStartsWith("-----BEGIN PRIVATE KEY-----\n", $pair->privateKey);
        $this->assertStringEndsWith("-----END PRIVATE KEY-----\n", $pair->privateKey);
        $this->assertStringStartsWith("-----BEGIN PUBLIC KEY-----\n", $pair->publicKey);
        $this->assertStringEndsWith("-----END PUBLIC KEY-----\n", $pair->publicKey);
    }

    public function testCreatePemKeyCertificateWithPassword(): void
    {
        $password = 'test-password';
        $pair = $this->generator->createPemKeyCertificate($password);

        $this->assertStringStartsWith("-----BEGIN ENCRYPTED PRIVATE KEY-----\n", $pair->privateKey);
        $this->assertStringEndsWith("-----END ENCRYPTED PRIVATE KEY-----\n", $pair->privateKey);
        $this->assertStringStartsWith("-----BEGIN PUBLIC KEY-----\n", $pair->publicKey);
        $this->assertStringEndsWith("-----END PUBLIC KEY-----\n", $pair->publicKey);
    }

    public function testGeneratedKeysAreValid(): void
    {
        $pair = $this->generator->createPemKeyCertificate(null);

        // Verify the private key is valid
        $privateKey = openssl_pkey_get_private($pair->privateKey);
        $this->assertNotFalse($privateKey, 'Private key should be valid');

        // Verify the public key is valid
        $publicKey = openssl_pkey_get_public($pair->publicKey);
        $this->assertNotFalse($publicKey, 'Public key should be valid');

        // Clean up
        openssl_free_key($privateKey);
        openssl_free_key($publicKey);
    }
}
