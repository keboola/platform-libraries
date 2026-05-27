<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Attribute;

use Attribute;

/**
 * @deprecated Use {@see KubernetesServiceAccountAuth} instead. This is a 1:1
 *     alias kept for backwards compatibility; it behaves identically and also
 *     accepts the Kubernetes ServiceAccount JWT header.
 */
#[Attribute(Attribute::TARGET_CLASS | Attribute::TARGET_METHOD | Attribute::IS_REPEATABLE)]
class ManageApiTokenAuth extends KubernetesServiceAccountAuth
{
}
