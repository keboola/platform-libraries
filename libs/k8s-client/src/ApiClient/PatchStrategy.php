<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ApiClient;

use KubernetesRuntime\APIPatchOperation;

enum PatchStrategy: string
{
    case JsonPatch = APIPatchOperation::PATCH;
    case JsonMergePatch = APIPatchOperation::MERGE_PATCH;
    case StrategicMergePatch = APIPatchOperation::STRATEGIC_MERGE_PATCH;
}
