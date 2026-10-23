<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication\Authenticator\Internal;

use Keboola\AzureApiClient\Authentication\Authenticator\RequestAuthenticatorInterface;
use Psr\Http\Message\RequestInterface;

/**
 * Signs every request with a SAS token scoped to the URI of the resource it accesses (scheme, host and path),
 * encoded the way Microsoft's reference implementation does it: lowercased, with lowercase percent-escapes.
 * https://learn.microsoft.com/en-us/rest/api/eventhub/generate-sas-token#php
 */
class SharedAccessSignatureAuthenticator implements RequestAuthenticatorInterface
{
    public function __construct(
        private readonly string $keyName,
        private readonly string $key,
        private readonly int $tokenTtlSeconds,
    ) {
    }

    public function __invoke(RequestInterface $request): RequestInterface
    {
        $uri = $request->getUri();
        $resourceUri = sprintf('%s://%s%s', $uri->getScheme(), $uri->getHost(), $uri->getPath());

        return $request->withHeader('Authorization', $this->createToken($resourceUri));
    }

    private function createToken(string $resourceUri): string
    {
        $encodedResourceUri = strtolower(rawurlencode(strtolower($resourceUri)));
        $expiry = time() + $this->tokenTtlSeconds;
        $signature = base64_encode(hash_hmac('sha256', $encodedResourceUri . "\n" . $expiry, $this->key, true));

        return sprintf(
            'SharedAccessSignature sr=%s&sig=%s&se=%d&skn=%s',
            $encodedResourceUri,
            self::lowercaseEscapes(rawurlencode($signature)),
            $expiry,
            rawurlencode($this->keyName),
        );
    }

    private static function lowercaseEscapes(string $encoded): string
    {
        return (string) preg_replace_callback(
            '~%[0-9A-F]{2}~',
            static fn(array $match): string => strtolower($match[0]),
            $encoded,
        );
    }
}
