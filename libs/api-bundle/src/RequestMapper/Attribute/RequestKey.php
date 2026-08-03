<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\RequestMapper\Attribute;

use Attribute;

/**
 * Declares the wire key a payload property is read from, for fields whose key cannot be a PHP
 * property name — typically the Keboola `#` prefix marking an encrypted value:
 *
 * ```php
 * final readonly class CreateCredentialsPayload
 * {
 *     public function __construct(
 *         #[RequestKey('#data')]
 *         public string $data,
 *     ) {}
 * }
 * ```
 *
 * Validation errors are reported under the wire key rather than the property name, so the client
 * sees the field it actually sent. A value posted directly under the property name (`data` above)
 * is dropped — only the declared wire key may populate the property, so the outcome never depends
 * on the order of keys in the request.
 *
 * Supported on top-level payload properties only: the key map is applied to the payload root, so a
 * declaration on a nested payload object is rejected with a
 * {@see \Keboola\ApiBundle\RequestMapper\Exception\RequestMapperException} rather than being
 * silently ignored.
 */
#[Attribute(Attribute::TARGET_PROPERTY | Attribute::TARGET_PARAMETER)]
class RequestKey
{
    /**
     * @param non-empty-string $key Key the value arrives under in the request.
     */
    public function __construct(
        public readonly string $key,
    ) {
    }
}
