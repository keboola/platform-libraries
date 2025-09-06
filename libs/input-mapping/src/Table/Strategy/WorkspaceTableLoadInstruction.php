<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Strategy;

use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;

final class WorkspaceTableLoadInstruction
{
    public function __construct(
        public readonly WorkspaceLoadType $loadType,
        public readonly RewrittenInputTableOptions $table,
        public readonly ?array $loadOptions,
    ) {
    }
}
