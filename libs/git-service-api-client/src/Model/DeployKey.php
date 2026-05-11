<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient\Model;

use Keboola\GitServiceApiClient\KeyPermission;
use Keboola\GitServiceApiClient\ResponseModelInterface;
use Webmozart\Assert\Assert;

final readonly class DeployKey implements ResponseModelInterface
{
    public function __construct(
        public string $id,
        public string $createdAt,
        public KeyPermission $permissions,
    ) {
    }

    public static function fromResponseData(array $data): static
    {
        Assert::keyExists($data, 'id');
        Assert::keyExists($data, 'createdAt');
        Assert::keyExists($data, 'permissions');
        Assert::string($data['id']);
        Assert::string($data['createdAt']);
        Assert::string($data['permissions']);

        return new self(
            $data['id'],
            $data['createdAt'],
            KeyPermission::from($data['permissions']),
        );
    }
}
