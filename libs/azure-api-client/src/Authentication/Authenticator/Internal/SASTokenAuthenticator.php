<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication\Authenticator\Internal;

use Keboola\AzureApiClient\Authentication\Authenticator\RequestAuthenticatorInterface;
use Psr\Http\Message\RequestInterface;
use Webmozart\Assert\Assert;

class SASTokenAuthenticator implements RequestAuthenticatorInterface
{
    private const SAS_AUTHORIZATION = 'SharedAccessSignature sig=%s&se=%s&skn=%s&sr=%s';
    private const EXPIRATION_TIME = 3600; // seconds

    public function __construct(
        private readonly string $url,
        private readonly string $sharedAccessKeyName,
        private readonly string $sharedAccessKey
    ) {
    }

    public function __invoke(RequestInterface $request): RequestInterface
    {
        return $request->withHeader('Authorization', $this->getAuthorization());
    }

    private function getAuthorization(): string
    {
        $expiry = time() + self::EXPIRATION_TIME;
        $encodedUrl = $this->lowerUrlencode($this->url);
        $scope = $encodedUrl . "\n" . $expiry;
        $signature = base64_encode(hash_hmac('sha256', $scope, $this->sharedAccessKey, true));
        return sprintf(
            self::SAS_AUTHORIZATION,
            $this->lowerUrlencode($signature),
            $expiry,
            $this->sharedAccessKeyName,
            $encodedUrl
        );
    }

    private function lowerUrlencode(string $str): string
    {
        $url = preg_replace_callback(
            '/%[0-9A-F]{2}/',
            static fn(array $matches) => strtolower($matches[0]),
            urlencode($str)
        );
        Assert::notNull($url);
        return $url;
    }
}
