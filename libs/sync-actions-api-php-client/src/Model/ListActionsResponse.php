<?php

declare(strict_types=1);

namespace Keboola\SyncActionsClient\Model;

use InvalidArgumentException;
use Keboola\ApiClientBase\ResponseModelInterface;

final readonly class ListActionsResponse implements ResponseModelInterface
{
    /**
     * @param array<string> $actions
     */
    public function __construct(
        public array $actions,
    ) {
    }

    public static function fromResponseData(array $data): static
    {
        if (!isset($data['actions']) || !is_array($data['actions'])) {
            throw new InvalidArgumentException('Response does not contain an "actions" array');
        }

        /** @var array<string> $actions */
        $actions = $data['actions'];

        return new self($actions);
    }
}
