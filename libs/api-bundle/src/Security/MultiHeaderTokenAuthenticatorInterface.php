<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Security;

/**
 * Extension of TokenAuthenticatorInterface that supports multiple authentication headers.
 * When an authenticator implements this interface, the AttributeAuthenticator will try
 * each header in order until it finds a non-null value.
 *
 * @template TokenType of TokenInterface
 * @extends TokenAuthenticatorInterface<TokenType>
 */
interface MultiHeaderTokenAuthenticatorInterface extends TokenAuthenticatorInterface
{
    /**
     * Returns a list of authentication headers to try, in priority order.
     * The first header that has a non-null value will be used for authentication.
     *
     * @return list<string>
     */
    public function getTokenHeaders(): array;
}
