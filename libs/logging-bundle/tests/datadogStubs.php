<?php

declare(strict_types=1);

namespace DDTrace;

// phpcs:disable
if (function_exists('DDTrace\\current_context')) {
    return;
}
// phpcs:enable

function current_context(): array
{
    return [
        'trace_id' => 'traceId',
        'span_id' => 'spanId',
    ];
}
