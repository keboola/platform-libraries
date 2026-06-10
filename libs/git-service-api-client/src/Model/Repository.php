<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient\Model;

use Keboola\ApiClientBase\ResponseModelInterface;
use Webmozart\Assert\Assert;

final readonly class Repository implements ResponseModelInterface
{
    public function __construct(
        public string $name,
        public string $createdAt,
        public string $defaultBranch,
        public string $sshUrl,
        public string $httpsUrl,
    ) {
    }

    public static function fromResponseData(array $data): static
    {
        Assert::keyExists($data, 'name');
        Assert::keyExists($data, 'createdAt');
        Assert::keyExists($data, 'defaultBranch');
        Assert::keyExists($data, 'sshUrl');
        Assert::keyExists($data, 'httpsUrl');
        Assert::string($data['name']);
        Assert::string($data['createdAt']);
        Assert::string($data['defaultBranch']);
        Assert::string($data['sshUrl']);
        Assert::string($data['httpsUrl']);

        return new self(
            $data['name'],
            $data['createdAt'],
            $data['defaultBranch'],
            $data['sshUrl'],
            $data['httpsUrl'],
        );
    }
}
