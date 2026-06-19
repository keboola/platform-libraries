<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient\Model;

use Keboola\ApiClientBase\ResponseModelInterface;
use Webmozart\Assert\Assert;

/**
 * Internal wrapper for `GET /repos/{name}/credentials` which returns `{credentials: [...]}`.
 * Not part of the public client surface — callers receive `list<Credential>`.
 *
 * @internal
 */
final readonly class CredentialListWrapper implements ResponseModelInterface
{
    /**
     * @param list<Credential> $credentials
     */
    public function __construct(public array $credentials)
    {
    }

    public static function fromResponseData(array $data): static
    {
        Assert::keyExists($data, 'credentials');
        Assert::isArray($data['credentials']);

        $credentials = [];
        foreach ($data['credentials'] as $item) {
            Assert::isArray($item);
            $credentials[] = Credential::fromResponseData($item);
        }

        return new self($credentials);
    }
}
