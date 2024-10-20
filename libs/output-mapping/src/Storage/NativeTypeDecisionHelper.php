<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Storage;

use Keboola\StorageApiBranch\StorageApiToken;

class NativeTypeDecisionHelper
{
    public static function shouldEnforceBaseTypes(StorageApiToken $token, string $backend): bool
    {
        return !$token->hasFeature('bigquery-native-types') && $backend === 'bigquery';
    }
}
