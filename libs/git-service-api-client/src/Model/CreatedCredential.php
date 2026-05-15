<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient\Model;

use Keboola\GitServiceApiClient\CredentialType;
use Keboola\GitServiceApiClient\KeyPermission;
use Webmozart\Assert\Assert;

/**
 * Response from `POST /repos/{name}/credentials`. Carries the same fields as {@see Credential}
 * plus the one-time fields returned only at creation: `secret` (HTTP token bearer) and `httpsUrl`
 * for `http_token` credentials.
 */
final readonly class CreatedCredential extends Credential
{
    public function __construct(
        string $id,
        CredentialType $type,
        string $username,
        ?string $publicKey,
        KeyPermission $permissions,
        string $createdAt,
        public ?string $secret,
        public ?string $httpsUrl,
    ) {
        parent::__construct($id, $type, $username, $publicKey, $permissions, $createdAt);
    }

    public static function fromResponseData(array $data): static
    {
        $base = self::parseBaseFields($data);

        $secret = $data['secret'] ?? null;
        $httpsUrl = $data['httpsUrl'] ?? null;
        Assert::nullOrString($secret);
        Assert::nullOrString($httpsUrl);

        return new self(
            $base['id'],
            $base['type'],
            $base['username'],
            $base['publicKey'],
            $base['permissions'],
            $base['createdAt'],
            $secret,
            $httpsUrl,
        );
    }
}
