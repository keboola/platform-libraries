<?php

declare(strict_types=1);

namespace Keboola\KeyGenerator;

use OpenSSLAsymmetricKey;
use RuntimeException;
use SensitiveParameter;

class PemKeyCertificateGenerator
{
    public function createPemKeyCertificate(#[SensitiveParameter] string|null $password): PemKeyCertificatePair
    {
        $res = openssl_pkey_new([
            'private_key_bits' => 2048,
            'private_key_type' => OPENSSL_KEYTYPE_RSA,
        ]);
        if ($res === false) {
            throw new RuntimeException('Failed to generate a new private key');
        }

        // Extract the private key
        openssl_pkey_export($res, $privateKey, $password);
        assert(is_string($privateKey));

        // Generate a Certificate Signing Request (CSR)
        $csr = openssl_csr_new((CertificateSigningRequest::createDefault())->toArray(), $res);
        assert($res instanceof OpenSSLAsymmetricKey);

        if ($csr === false) {
            throw new RuntimeException('Failed to generate a new CSR');
        }
        if ($csr === true) {
            throw new RuntimeException('New CSR generated, but signing failed');
        }

        // Self-sign the CSR to create the certificate
        $cert = openssl_csr_sign($csr, null, $res, 365);
        if ($cert === false) {
            throw new RuntimeException('Failed to sign the CSR');
        }

        openssl_pkey_export($res, $privateKeyPem);

        // Extraction of public key
        $details = openssl_pkey_get_details($res);
        if ($details === false) {
            throw new RuntimeException('Failed to get details of the private key');
        }

        $publicKeyPem = $details['key'] ?? null;
        if (!is_string($publicKeyPem)) {
            throw new RuntimeException('Failed to get the public key from the private key');
        }

        return new PemKeyCertificatePair($privateKey, $publicKeyPem);
    }
}
