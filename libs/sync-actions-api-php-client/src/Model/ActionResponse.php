<?php

declare(strict_types=1);

namespace Keboola\SyncActionsClient\Model;

use stdClass;

/**
 * Wraps the raw, component-defined payload returned by a sync action. The payload is decoded
 * as a deep stdClass (see SyncActionsApiClient::callAction) so callers can navigate it as
 * $response->data->whatever, preserving the object/array distinction the API returns.
 */
final readonly class ActionResponse
{
    public function __construct(
        public stdClass $data,
    ) {
    }
}
