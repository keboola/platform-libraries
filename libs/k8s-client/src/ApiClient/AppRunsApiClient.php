<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ApiClient;

use Keboola\K8sClient\BaseApi\AppRun as AppRunsApi;
use Keboola\K8sClient\KubernetesApiClient;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\AppRun;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\AppRunList;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Patch;

/**
 * @template-extends BaseNamespaceApiClient<AppRunsApi, AppRunList, AppRun>
 */
class AppRunsApiClient extends BaseNamespaceApiClient
{
    public function __construct(KubernetesApiClient $apiClient)
    {
        parent::__construct(
            $apiClient,
            new AppRunsApi(),
            AppRunList::class,
            AppRun::class,
        );
    }

    /**
     * Patch an AppRun using merge-patch strategy
     */
    public function patch(
        string $name,
        Patch $model,
        array $queries = [],
        PatchStrategy $strategy = PatchStrategy::JsonMergePatch,
    ): AppRun {
        $data = $model->getArrayCopy();
        $data['patchOperation'] = $strategy->value;

        /** @var AppRun $appRun */
        $appRun = parent::patch($name, new Patch($data), $queries);

        return $appRun;
    }
}
