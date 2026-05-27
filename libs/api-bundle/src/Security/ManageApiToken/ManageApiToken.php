<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Security\ManageApiToken;

use Keboola\ApiBundle\Security\KubernetesServiceAccount\KubernetesServiceAccountToken;

/*
 * @deprecated Backwards-compatibility alias for {@see KubernetesServiceAccountToken}.
 *
 * The authenticator produces KubernetesServiceAccountToken instances; this alias
 * keeps the old FQN resolving so existing `#[CurrentUser] ManageApiToken $token`
 * type-hints keep accepting them. Use KubernetesServiceAccountToken directly.
 */
class_alias(
    KubernetesServiceAccountToken::class,
    'Keboola\\ApiBundle\\Security\\ManageApiToken\\ManageApiToken',
);
