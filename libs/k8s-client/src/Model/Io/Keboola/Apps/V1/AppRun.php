<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V1;

use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\ObjectMeta;
use KubernetesRuntime\AbstractModel;

/**
 * AppRun is a custom resource definition for tracking Pod lifecycles for cost tracking
 *
 * @property string|null $apiVersion
 * @property string|null $kind
 * @property ObjectMeta|null $metadata
 * @property AppRunSpec|null $spec
 * @property AppRunStatus|null $status
 */
class AppRun extends AbstractModel
{
    /**
     * APIVersion defines the versioned schema of this representation of an object.
     */
    public string|null $apiVersion = 'apps.keboola.com/v1';

    /**
     * Kind is a string value representing the REST resource this object represents.
     */
    public string|null $kind = 'AppRun';

    /**
     * Standard object's metadata
     */
    public ObjectMeta|null $metadata = null;

    /**
     * Specification of the desired state of the AppRun
     */
    public AppRunSpec|null $spec = null;

    /**
     * Most recently observed status of the AppRun
     */
    public AppRunStatus|null $status = null;
}
