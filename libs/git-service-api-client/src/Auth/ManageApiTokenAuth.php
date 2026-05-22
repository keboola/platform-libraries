<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient\Auth;

use SensitiveParameter;
use Webmozart\Assert\Assert;

final readonly class ManageApiTokenAuth implements AuthInterface
{
    /**
     * @param non-empty-string $token
     */
    public function __construct(
        #[SensitiveParameter]
        private string $token,
    ) {
        Assert::stringNotEmpty($token, 'Manage API token must not be empty');
    }

    public function getAuthenticationHeaders(): array
    {
        return [
            'X-KBC-ManageApiToken' => $this->token,
        ];
    }
}
