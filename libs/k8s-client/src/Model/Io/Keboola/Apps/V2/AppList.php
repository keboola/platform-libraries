<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V2;

use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\ListMeta;
use KubernetesRuntime\AbstractModel;

/**
 * AppList is a list of App resources
 *
 * @property string|null $apiVersion
 * @property string|null $kind
 * @property ListMeta|null $metadata
 * @property App[] $items
 */
class AppList extends AbstractModel
{
    /**
     * APIVersion defines the versioned schema of this representation of an object
     */
    public string|null $apiVersion = 'apps.keboola.com/v1';

    /**
     * Kind is a string value representing the REST resource this object represents
     */
    public string|null $kind = 'AppList';

    /**
     * Standard list metadata
     */
    public ListMeta|null $metadata = null;

    /**
     * List of apps
     *
     * @var App[]
     */
    public array $items = [];
}
