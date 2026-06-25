<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient\Model;

use Keboola\ApiClientBase\ResponseModelInterface;
use Webmozart\Assert\Assert;

final readonly class Commit implements ResponseModelInterface
{
    public function __construct(
        public string $sha,
        public string $created,
        public string $message,
        public CommitAuthor $author,
    ) {
    }

    public static function fromResponseData(array $data): static
    {
        Assert::keyExists($data, 'sha');
        Assert::keyExists($data, 'created');
        Assert::keyExists($data, 'message');
        Assert::keyExists($data, 'author');
        Assert::string($data['sha']);
        Assert::string($data['created']);
        Assert::string($data['message']);
        Assert::isArray($data['author']);

        return new self(
            $data['sha'],
            $data['created'],
            $data['message'],
            CommitAuthor::fromResponseData($data['author']),
        );
    }
}
