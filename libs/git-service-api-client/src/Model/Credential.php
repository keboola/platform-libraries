<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient\Model;

use Keboola\ApiClientBase\ResponseModelInterface;
use Keboola\GitServiceApiClient\CredentialType;
use Keboola\GitServiceApiClient\KeyPermission;
use Webmozart\Assert\Assert;

readonly class Credential implements ResponseModelInterface
{
    public function __construct(
        public string $id,
        public CredentialType $type,
        public string $username,
        public ?string $publicKey,
        public KeyPermission $permissions,
        public string $createdAt,
    ) {
    }

    public static function fromResponseData(array $data): static
    {
        $base = self::parseBaseFields($data);

        // `new self` returns a Credential. Subclasses override fromResponseData and never delegate
        // back into this body, so the `static` contract holds for callers that actually reach here.
        // @phpstan-ignore return.type
        return new self(
            $base['id'],
            $base['type'],
            $base['username'],
            $base['publicKey'],
            $base['permissions'],
            $base['createdAt'],
        );
    }

    /**
     * @param array<string, mixed> $data
     * @return array{
     *     id: string,
     *     type: CredentialType,
     *     username: string,
     *     publicKey: ?string,
     *     permissions: KeyPermission,
     *     createdAt: string,
     * }
     */
    protected static function parseBaseFields(array $data): array
    {
        Assert::keyExists($data, 'id');
        Assert::keyExists($data, 'type');
        Assert::keyExists($data, 'username');
        Assert::keyExists($data, 'permissions');
        Assert::keyExists($data, 'createdAt');
        Assert::stringNotEmpty($data['id']);
        Assert::string($data['type']);
        Assert::string($data['username']);
        Assert::string($data['permissions']);
        Assert::string($data['createdAt']);

        $publicKey = $data['publicKey'] ?? null;
        Assert::nullOrString($publicKey);

        return [
            'id' => $data['id'],
            'type' => CredentialType::from($data['type']),
            'username' => $data['username'],
            'publicKey' => $publicKey,
            'permissions' => KeyPermission::from($data['permissions']),
            'createdAt' => $data['createdAt'],
        ];
    }
}
