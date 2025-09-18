<?php

declare(strict_types=1);

namespace Keboola\SyncActionsClient\Model;

use stdClass;

interface ResponseModelInterface
{
    public static function fromResponseData(stdClass $data): static;
}
