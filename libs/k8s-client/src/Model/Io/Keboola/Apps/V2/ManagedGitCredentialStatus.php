<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V2;

use KubernetesRuntime\AbstractModel;

/**
 * ManagedGitCredentialStatus holds the active credential identifier for the current
 * AppRun, populated when the operator successfully registers an ephemeral credential
 * with git-service and cleared when revoked.
 */
class ManagedGitCredentialStatus extends AbstractModel
{
    /**
     * Id is the credential ID (Forgejo user ID) returned by the git-service credentials API.
     *
     * @var string|null
     */
    public $id = null;

    /**
     * RepoId is the git-service repository ID for which this credential was minted.
     * Persisted here so the credential can be revoked even if managedGitRepo is removed from spec.
     *
     * @var string|null
     */
    public $repoId = null;
}
