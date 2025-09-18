<?php

declare(strict_types=1);

namespace Keboola\SyncActionsClient\Model;

use stdClass;

final readonly class ListActionsResponse implements ResponseModelInterface
{
    /**
     * @param array<string> $actions
     */
    public function __construct(
        public array $actions,
    ) {
    }

    public static function fromResponseData(stdClass $data): static
    {
        return new self(
            // @phpstan-ignore-next-line
            $data->actions,
        );
    }
}
