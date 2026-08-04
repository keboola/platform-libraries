<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\RequestMapper;

/**
 * Wire format of a request payload, as declared by {@see Attribute\RequestPayloadObject}.
 *
 * The case values match Symfony's request format names, so they can be compared directly
 * against {@see \Symfony\Component\HttpFoundation\Request::getContentTypeFormat()}.
 */
enum PayloadFormat: string
{
    /** `application/json` request body. */
    case Json = 'json';

    /** `application/x-www-form-urlencoded` or `multipart/form-data` request body. */
    case Form = 'form';

    /**
     * Content type advertised to the client when the request does not match this format.
     */
    public function contentType(): string
    {
        return match ($this) {
            self::Json => 'application/json',
            self::Form => 'application/x-www-form-urlencoded',
        };
    }
}
