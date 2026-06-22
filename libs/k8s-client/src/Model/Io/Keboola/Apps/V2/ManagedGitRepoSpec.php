<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V2;

use KubernetesRuntime\AbstractModel;

/**
 * ManagedGitRepoSpec describes a managed Git repository whose credential is minted per AppRun.
 */
class ManagedGitRepoSpec extends AbstractModel
{
    /**
     * RepoId is the git-service identifier for the managed repository.
     *
     * @var string|null
     */
    public $repoId = null;

    /**
     * CredentialType selects the credential format minted for each AppRun.
     * "http_token" overlays username + #password into config.json.
     * "ssh_key" generates an Ed25519 keypair and overlays #sshKey (PEM) into config.json.
     *
     * @var string|null
     */
    public $credentialType = null;
}
