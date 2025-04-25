<?php

declare(strict_types=1);

namespace Keboola\KeyGenerator;

readonly class CertificateSigningRequest
{
    public function __construct(
        private string $countryName,
        private string $stateOrProvinceName,
        private string $localityName,
        private string $organizationName,
        private string $commonName,
        private string $emailAddress,
        private string $businessCategory,
        private string $jurisdictionCountryName,
        private string $jurisdictionStateOrProvinceName,
        private string $serialNumber,
    ) {
    }

    public static function createDefault(): self
    {
        return new self(
            countryName: 'CZ',
            stateOrProvinceName: 'Prague',
            localityName: 'Prague 7',
            organizationName: 'Keboola Czech s.r.o.',
            commonName: 'keboola.com',
            emailAddress: 'support@keboola.com',
            businessCategory: 'Private Organization',
            jurisdictionCountryName: 'CZ',
            jurisdictionStateOrProvinceName: 'Prague',
            serialNumber: '28502787',
        );
    }

    /**
     * @return array{
     *     countryName: string,
     *     stateOrProvinceName: string,
     *     localityName: string,
     *     organizationName: string,
     *     commonName: string,
     *     emailAddress: string,
     *     businessCategory: string,
     *     jurisdictionCountryName: string,
     *     jurisdictionStateOrProvinceName: string,
     *     serialNumber: string
     * }
     */
    public function toArray(): array
    {
        return [
            'countryName' => $this->countryName,
            'stateOrProvinceName' => $this->stateOrProvinceName,
            'localityName' => $this->localityName,
            'organizationName' => $this->organizationName,
            'commonName' => $this->commonName,
            'emailAddress' => $this->emailAddress,
            'businessCategory' => $this->businessCategory,
            'jurisdictionCountryName' => $this->jurisdictionCountryName,
            'jurisdictionStateOrProvinceName' => $this->jurisdictionStateOrProvinceName,
            'serialNumber' => $this->serialNumber,
        ];
    }
}
