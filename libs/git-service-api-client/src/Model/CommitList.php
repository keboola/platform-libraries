<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient\Model;

use Keboola\ApiClientBase\ResponseModelInterface;
use Webmozart\Assert\Assert;

/**
 * Wrapper for `GET /repos/{name}/refs/{ref}/commits` which returns `{commits: [...], total: N}`.
 *
 * `total` is the total number of commits reachable from the requested ref (the full history
 * length), not the size of the returned page.
 */
final readonly class CommitList implements ResponseModelInterface
{
    /**
     * @param list<Commit> $commits
     */
    public function __construct(
        public array $commits,
        public int $total,
    ) {
    }

    public static function fromResponseData(array $data): static
    {
        Assert::keyExists($data, 'commits');
        Assert::keyExists($data, 'total');
        Assert::isArray($data['commits']);
        Assert::integer($data['total']);

        $commits = [];
        foreach ($data['commits'] as $item) {
            Assert::isArray($item);
            $commits[] = Commit::fromResponseData($item);
        }

        return new self($commits, $data['total']);
    }
}
