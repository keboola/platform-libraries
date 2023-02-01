<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Marketplace;

enum OperationStatus: string
{
    case SUCCESS = 'Success';
    case FAILURE = 'Failure';
}
