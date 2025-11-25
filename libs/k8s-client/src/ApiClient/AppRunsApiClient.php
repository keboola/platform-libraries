<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ApiClient;

use Keboola\K8sClient\BaseApi\AppRun as AppRunsApi;
use Keboola\K8sClient\KubernetesApiClient;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\AppRun;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\AppRunList;

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
}
