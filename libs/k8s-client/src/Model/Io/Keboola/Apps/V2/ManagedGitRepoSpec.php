<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V2;

use KubernetesRuntime\AbstractModel;

/**
 * ManagedGitRepoSpec describes a managed Git repository whose deploy key is minted per AppRun.
 */
class ManagedGitRepoSpec extends AbstractModel
{
    /**
     * RepoId is the git-service identifier for the managed repository.
     * When set, the operator generates an ephemeral SSH keypair per AppRun,
     * registers the public key with git-service, overlays the plaintext private
     * key into the rendered config.json under parameters.dataApp.git['#sshKey'],
     * and revokes the key when the AppRun reaches a terminal state.
     *
     * @var string|null
     */
    public $repoId = null;
}
