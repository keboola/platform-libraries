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
     */
    public string|null $name = null;

    /**
     * UID is the unique identifier of the Pod
     */
    public string|null $uid = null;
}
