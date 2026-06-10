<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Auth;

use SensitiveParameter;
use Webmozart\Assert\Assert;

final readonly class StorageApiTokenAuthenticator implements RequestAuthenticatorInterface
{
    public const HEADER = 'X-StorageApi-Token';

    /**
     * @param non-empty-string $token
     */
    public function __construct(
        #[SensitiveParameter]
        private string $token,
    ) {
        Assert::stringNotEmpty($token, 'Storage API token must not be empty');
    }

    public function getAuthenticationHeaders(): array
    {
        return [self::HEADER => $this->token];
    }
}
