<?php

declare(strict_types=1);

namespace Keboola\SyncActionsClient\Model;

use stdClass;

final readonly class ActionResponse implements ResponseModelInterface
{
    public function __construct(
        public stdClass $data,
    ) {
    }

    public static function fromResponseData(stdClass $data): static
    {
        return new self($data);
    }
}
