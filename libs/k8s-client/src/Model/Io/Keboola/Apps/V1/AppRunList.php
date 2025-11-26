<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V1;

use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\ListMeta;
use KubernetesRuntime\AbstractModel;

/**
 * AppRunList contains a list of AppRun
 *
 * @property string|null $apiVersion
 * @property string|null $kind
 * @property ListMeta|null $metadata
 * @property array<AppRun> $items
 */
class AppRunList extends AbstractModel
{
    /**
     * @var string|null
     */
    public $apiVersion = 'apps.keboola.com/v1';

    /**
     * @var string|null
     */
    public $kind = 'AppRunList';

    /**
     * @var ListMeta|null
     */
    public $metadata = null;

    /**
     * @var array<AppRun>
     */
    public $items = [];
}
