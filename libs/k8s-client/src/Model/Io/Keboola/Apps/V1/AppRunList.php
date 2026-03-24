<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V1;

use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\ListMeta;
use KubernetesRuntime\AbstractModel;

/**
 * AppRunList contains a list of AppRun
 */
class AppRunList extends AbstractModel
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
    public $kind = 'AppRunList';

    /**
     * Standard list metadata
     *
     * @var ListMeta
     */
    public $metadata = null;

    /**
     * List of AppRuns
     *
     * @var AppRun[]
     */
    public $items = null;
}
