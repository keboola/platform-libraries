<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle;

enum Platform: string
{
    case GCP = 'gcp';
    case AWS = 'aws';
    case AZURE = 'azure';
}
