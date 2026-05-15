<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient;

use Webmozart\Assert\Assert;

/**
 * Body for `POST /repos/{name}/credentials`. The git-service endpoint accepts a single
 * shape discriminated by `type`, but the valid `publicKey` presence depends on the type:
 * required for `ssh_key`, forbidden for `http_token`. The private constructor + named
 * factories ({@see self::sshKey()}, {@see self::httpToken()}) make those combinations
 * impossible to misuse.
 */
final readonly class NewCredential
{
    /**
     * @param non-empty-string $username
     * @param non-empty-string|null $publicKey
     */
    private function __construct(
        public CredentialType $type,
        public string $username,
        public ?string $publicKey,
        public KeyPermission $permissions,
    ) {
        Assert::stringNotEmpty($username, 'username must not be empty');

        match ($type) {
            CredentialType::SshKey => Assert::stringNotEmpty(
                $publicKey,
                'publicKey must not be empty for SSH key credentials',
            ),
            CredentialType::HttpToken => Assert::null(
                $publicKey,
                'publicKey must not be set for HTTP token credentials',
            ),
        };
    }

    /**
     * @param non-empty-string $username
     * @param non-empty-string $publicKey
     */
    public static function sshKey(string $username, string $publicKey, KeyPermission $permissions): self
    {
        return new self(CredentialType::SshKey, $username, $publicKey, $permissions);
    }

    /**
     * @param non-empty-string $username
     */
    public static function httpToken(string $username, KeyPermission $permissions): self
    {
        return new self(CredentialType::HttpToken, $username, null, $permissions);
    }

    /**
     * @return array<string, string>
     */
    public function toRequestBody(): array
    {
        $body = [
            'type' => $this->type->value,
            'username' => $this->username,
            'permissions' => $this->permissions->value,
        ];
        if ($this->publicKey !== null) {
            $body['publicKey'] = $this->publicKey;
        }
        return $body;
    }
}
