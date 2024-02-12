<?php

declare(strict_types=1);

namespace Keboola\SandboxesServiceApiClient;

use JsonException;

class Json
{
    public static function encodeArray(array $data): string
    {
        return (string) json_encode($data, JSON_THROW_ON_ERROR);
    }

    public static function decodeArray(string $data): array
    {
        $result = json_decode($data, true, flags: JSON_THROW_ON_ERROR);
        if (!is_array($result)) {
            throw new JsonException(sprintf('Decoded data is %s, array expected', get_debug_type($result)));
        }

        return $result;
    }
}
