<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Storage;

class NativeTypeDecisionHelper
{
    public static function shouldEnforceBaseTypes(bool $hasBigQueryNativeTypesFeature, string $backend): bool
    {
        return !$hasBigQueryNativeTypesFeature && $backend === 'bigquery';
    }
}
