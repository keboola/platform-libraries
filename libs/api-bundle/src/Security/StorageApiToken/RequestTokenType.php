<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Security\StorageApiToken;

/**
 * The kind of token an incoming request carries, as classified from its headers by
 * {@see StorageApiTokenAuthenticator}. Each case maps to exactly one handling method on
 * {@see StorageApiTokenFactory}.
 */
enum RequestTokenType
{
    /** Legacy Storage token: `X-StorageApi-Token`, or a non-Bearer `Authorization` value. */
    case StorageToken;

    /** OAuth bearer token: `Authorization: Bearer <token>` that is not a programmatic token. */
    case OAuthToken;

    /** Connection programmatic token: `Authorization: Bearer <kbc_at_*|kbc_pat_*>`. */
    case Programmatic;
}
