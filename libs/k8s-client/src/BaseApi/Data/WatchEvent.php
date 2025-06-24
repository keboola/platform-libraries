<?php

declare(strict_types=1);

namespace Keboola\K8sClient\BaseApi\Data;

use Keboola\K8sClient\Util\StreamResponse;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Runtime\RawExtension;
use KubernetesRuntime\AbstractModel;

class WatchEvent
{
    public function __construct(
        public WatchEventType $type,
        public AbstractModel $object,
    ) {
    }

    public static function fromResponse(array $data): self
    {
        $eventType = WatchEventType::from($data['type']);
        $objectData = $data['object'] ?? [];

        return new WatchEvent(
            type: $eventType,
            object: match ($eventType) {
                WatchEventType::Error => new RawExtension($objectData),
                default => StreamResponse::instantiateResponseObject($objectData),
            },
        );
    }
}
