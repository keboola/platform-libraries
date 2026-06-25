<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient\Model;

use Keboola\ApiClientBase\ResponseModelInterface;
use Webmozart\Assert\Assert;

/**
 * Internal wrapper for `GET /repos/{name}/refs` which returns `{refs: [...]}`.
 * Not part of the public client surface — callers receive `list<GitRef>`.
 *
 * @internal
 */
final readonly class GitRefListWrapper implements ResponseModelInterface
{
    /**
     * @param list<GitRef> $refs
     */
    public function __construct(public array $refs)
    {
    }

    public static function fromResponseData(array $data): static
    {
        Assert::keyExists($data, 'refs');
        Assert::isArray($data['refs']);

        $refs = [];
        foreach ($data['refs'] as $item) {
            Assert::isArray($item);
            $refs[] = GitRef::fromResponseData($item);
        }

        return new self($refs);
    }
}
