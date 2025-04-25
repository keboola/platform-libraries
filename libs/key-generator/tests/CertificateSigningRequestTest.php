<?php

declare(strict_types=1);

namespace Keboola\KeyGenerator\Tests;

use Keboola\KeyGenerator\CertificateSigningRequest;
use PHPUnit\Framework\TestCase;

class CertificateSigningRequestTest extends TestCase
{
    public function testToArrayReturnsAllFields(): void
    {
        $csr = new CertificateSigningRequest(
            'US',
            'California',
            'San Francisco',
            'Test Org',
            'test.com',
            'test@test.com',
            'Test Category',
            'US',
            'California',
            '12345678',
        );

        self::assertSame([
            'countryName' => 'US',
            'stateOrProvinceName' => 'California',
            'localityName' => 'San Francisco',
            'organizationName' => 'Test Org',
            'commonName' => 'test.com',
            'emailAddress' => 'test@test.com',
            'businessCategory' => 'Test Category',
            'jurisdictionCountryName' => 'US',
            'jurisdictionStateOrProvinceName' => 'California',
            'serialNumber' => '12345678',
        ], $csr->toArray());
    }

    public function testCreateDefault(): void
    {
        $csr = CertificateSigningRequest::createDefault();

        self::assertSame([
            'countryName' => 'CZ',
            'stateOrProvinceName' => 'Prague',
            'localityName' => 'Prague 7',
            'organizationName' => 'Keboola Czech s.r.o.',
            'commonName' => 'keboola.com',
            'emailAddress' => 'support@keboola.com',
            'businessCategory' => 'Private Organization',
            'jurisdictionCountryName' => 'CZ',
            'jurisdictionStateOrProvinceName' => 'Prague',
            'serialNumber' => '28502787',
        ], $csr->toArray());
    }
}
