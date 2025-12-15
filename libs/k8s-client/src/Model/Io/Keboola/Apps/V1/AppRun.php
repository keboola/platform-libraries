<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V1;

use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\ObjectMeta;
use KubernetesRuntime\AbstractModel;

/**
 * AppRun is a custom resource definition for tracking Pod lifecycles for cost tracking
 *
 * @property string $apiVersion
 * @property string $kind
 * @property ObjectMeta $metadata
 * @property AppRunSpec $spec
 * @property AppRunStatus|null $status
 */
class AppRun extends AbstractModel
{
    /**
     * APIVersion defines the versioned schema of this representation of an object.
     *
     * @var string
     */
    public $apiVersion = 'apps.keboola.com/v1';

    /**
     * Kind is a string value representing the REST resource this object represents.
     *
     * @var string
     */
    public $kind = 'AppRun';

    /**
     * Standard object's metadata
     *
     * @var ObjectMeta
     */
    public $metadata = null;

    /**
     * Specification of the desired state of the AppRun
     *
     * @var AppRunSpec
     */
    public $spec = null;

    /**
     * Most recently observed status of the AppRun
     *
     * @var AppRunStatus|null
     */
    public $status = null;
}
