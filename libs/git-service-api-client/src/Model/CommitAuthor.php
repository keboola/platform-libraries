<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient\Model;

use Keboola\ApiClientBase\ResponseModelInterface;
use Webmozart\Assert\Assert;

/**
 * Git-level author of a commit (name/email/date), as exposed by git-service.
 * This is the git author, not the Forgejo user object.
 */
final readonly class CommitAuthor implements ResponseModelInterface
{
    public function __construct(
        public string $name,
        public string $email,
        public string $date,
    ) {
    }

    public static function fromResponseData(array $data): static
    {
        Assert::keyExists($data, 'name');
        Assert::keyExists($data, 'email');
        Assert::keyExists($data, 'date');
        Assert::string($data['name']);
        Assert::string($data['email']);
        Assert::string($data['date']);

        return new self(
            $data['name'],
            $data['email'],
            $data['date'],
        );
    }
}
