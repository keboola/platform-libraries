<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ApiClient;

use InvalidArgumentException;
use Keboola\K8sClient\BaseApi\App as AppsApi;
use Keboola\K8sClient\Exception\ResourceNotFoundException;
use Keboola\K8sClient\KubernetesApiClient;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\App;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\AppList;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Patch;
use KubernetesRuntime\APIPatchOperation;

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

    /**
     * Patch an App using a merge-patch strategy
     */
    public function patch(string $name, App|Patch $model, array $queries = []): App
    {
        $data = $model->getArrayCopy();
        $data['patchOperation'] = APIPatchOperation::MERGE_PATCH;

        /** @var App $app */
        $app = parent::patch($name, new Patch($data), $queries);

        return $app;
    }

    /**
     * Create or patch an app (patch if exists, create if not)
     */
    public function createOrPatch(App $app, array $queries = []): App
    {
        $name = $app->metadata?->name;
        if ($name === null) {
            throw new InvalidArgumentException('App metadata.name is required');
        }

        try {
            // Try to patch first
            return $this->patch($name, $app, $queries);
        } catch (ResourceNotFoundException) {
            // If not found, create it
            return $this->create($app, $queries);
        }
    }
}
