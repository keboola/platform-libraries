<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests;

use KubernetesRuntime\AbstractModel;

/**
 * Throwaway CRD model used across tests to exercise the extra-client registry without the
 * library owning any real CRD. Kept in its own file so it is PSR-4 autoloadable from any test.
 */
class FakeCrdModel extends AbstractModel
{
    /** @var string */
    public $apiVersion = 'example.keboola.com/v1';

    /** @var string */
    public $kind = 'FakeCrd';

    /** @var \Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\ObjectMeta|null */
    public $metadata = null;

    /** @var array<string, mixed>|null */
    public $spec = null;

    /** @var array<string, mixed>|null */
    public $status = null;
}
