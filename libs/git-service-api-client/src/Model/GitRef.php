<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient\Model;

use Keboola\ApiClientBase\ResponseModelInterface;
use Webmozart\Assert\Assert;

final readonly class GitRef implements ResponseModelInterface
{
    public function __construct(
        public string $ref,
        public string $sha,
        public string $type,
    ) {
    }

    public static function fromResponseData(array $data): static
    {
        Assert::keyExists($data, 'ref');
        Assert::keyExists($data, 'sha');
        Assert::keyExists($data, 'type');
        Assert::string($data['ref']);
        Assert::string($data['sha']);
        Assert::string($data['type']);

        return new self(
            $data['ref'],
            $data['sha'],
            $data['type'],
        );
    }
}
