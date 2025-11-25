<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ApiClient;

use Keboola\K8sClient\BaseApi\App as AppsApi;
use Keboola\K8sClient\KubernetesApiClient;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\App;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\AppList;

/**
 * @template-extends BaseNamespaceApiClient<AppsApi, AppList, App>
 */
class AppsApiClient extends BaseNamespaceApiClient
{
    public function __construct(KubernetesApiClient $apiClient)
    {
        parent::__construct(
            $apiClient,
            new AppsApi(),
            AppList::class,
            App::class,
        );
    }
}
