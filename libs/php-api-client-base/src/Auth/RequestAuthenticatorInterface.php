<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Auth;

interface RequestAuthenticatorInterface
{
    /**
     * Headers to add to every outbound request, keyed by header name.
     *
     * Symfony HttpClient applies auth via the request `headers` option rather than
     * by mutating an immutable PSR-7 request, so authenticators return a header map
     * instead of decorating a request. The map is resolved per request (see
     * {@see AuthenticatingHttpClient}), so file-/token-backed authenticators can
     * re-read rotated credentials on every call — including on retries.
     *
     * @return array<string, string>
     */
    public function getAuthenticationHeaders(): array;
}
