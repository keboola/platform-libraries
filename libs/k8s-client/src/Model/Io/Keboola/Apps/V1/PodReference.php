<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V1;

use KubernetesRuntime\AbstractModel;

/**
 * PodReference contains information to identify the Pod this AppRun tracks
 *
 * @property string|null $name
 * @property string|null $uid
 */
class PodReference extends AbstractModel
{
    /**
     * Name is the name of the Pod
     *
     * @var string|null
     */
    public $name = null;

    /**
     * UID is the unique identifier of the Pod
     *
     * @var string|null
     */
    public $uid = null;
}
