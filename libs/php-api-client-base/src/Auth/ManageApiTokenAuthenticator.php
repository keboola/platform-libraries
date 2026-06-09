<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Auth;

use Psr\Http\Message\RequestInterface;
use SensitiveParameter;
use Webmozart\Assert\Assert;

final readonly class ManageApiTokenAuthenticator implements RequestAuthenticatorInterface
{
    public const HEADER = 'X-KBC-ManageApiToken';

    /**
     * @param non-empty-string $token
     */
    public function __construct(
        #[SensitiveParameter]
        private string $token,
    ) {
        Assert::stringNotEmpty($token, 'Manage API token must not be empty');
    }

    public function __invoke(RequestInterface $request): RequestInterface
    {
        return $request->withHeader(self::HEADER, $this->token);
    }
}
