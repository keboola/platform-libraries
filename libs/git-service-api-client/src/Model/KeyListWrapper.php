<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient\Model;

use Keboola\GitServiceApiClient\ResponseModelInterface;
use Webmozart\Assert\Assert;

/**
 * Internal wrapper for `GET /repos/{name}/keys` which returns `{keys: [...]}`.
 * Not part of the public client surface — callers receive `list<DeployKey>`.
 *
 * @internal
 */
final readonly class KeyListWrapper implements ResponseModelInterface
{
    /**
     * @param list<DeployKey> $keys
     */
    public function __construct(public array $keys)
    {
    }

    public static function fromResponseData(array $data): static
    {
        Assert::keyExists($data, 'keys');
        Assert::isArray($data['keys']);

        $keys = [];
        foreach ($data['keys'] as $item) {
            Assert::isArray($item);
            $keys[] = DeployKey::fromResponseData($item);
        }

        return new self($keys);
    }
}
