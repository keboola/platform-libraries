<?php

declare(strict_types=1);

namespace Keboola\Toxiproxy;

enum StreamDirections: string
{
    case UPSTREAM = 'upstream';
    case DOWNSTREAM = 'downstream';
}
