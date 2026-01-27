<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V2;

use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\ObjectMeta;
use KubernetesRuntime\AbstractModel;

/**
 * App is a custom resource definition for Keboola applications
 *
 * @property string|null $apiVersion
 * @property string|null $kind
 * @property ObjectMeta|null $metadata
 * @property AppSpec|null $spec
 * @property AppStatus|null $status
 */
class App extends AbstractModel
{
    /**
     * APIVersion defines the versioned schema of this representation of an object.
     * Servers should convert recognized schemas to the latest internal value, and
     * may reject unrecognized values.
     */
    public string|null $apiVersion = 'apps.keboola.com/v1';

    /**
     * Kind is a string value representing the REST resource this object represents.
     * Servers may infer this from the endpoint the client submits requests to.
     */
    public string|null $kind = 'App';

    /**
     * Standard object's metadata
     */
    public ObjectMeta|null $metadata = null;

    /**
     * Specification of the desired behavior of the App
     */
    public AppSpec|null $spec = null;

    /**
     * Most recently observed status of the App
     */
    public AppStatus|null $status = null;
}
