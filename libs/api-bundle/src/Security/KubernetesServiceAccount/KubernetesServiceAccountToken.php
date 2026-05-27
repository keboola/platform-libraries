<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Security\KubernetesServiceAccount;

use Keboola\ApiBundle\Security\ManageApiToken\ManageApiToken;

/**
 * Token produced when authenticating with a Manage API token or a Kubernetes
 * ServiceAccount JWT. Extends the deprecated {@see ManageApiToken} purely so the
 * old type-hint remains a valid supertype during the deprecation period.
 */
class KubernetesServiceAccountToken extends ManageApiToken
{
}
