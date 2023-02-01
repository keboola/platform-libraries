<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication;

use Keboola\AzureApiClient\GuzzleClientFactory;

interface AuthenticatorInterface
{
    public function __construct(GuzzleClientFactory $clientFactory);

    public function getAuthenticationToken(string $resource): string;

    public function checkUsability(): void;
}
