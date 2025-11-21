<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ApiClient;

use Generator;
use Keboola\K8sClient\BaseApi\AppRun as AppRunsApi;
use Keboola\K8sClient\KubernetesApiClient;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\AppRun;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\AppRunList;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Patch;
use KubernetesRuntime\AbstractModel;
use KubernetesRuntime\APIPatchOperation;

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
    public function patch(string $name, Patch $model, array $queries = []): AppRun
    {
        $data = $model->getArrayCopy();
        $data['patchOperation'] = APIPatchOperation::MERGE_PATCH;

        /** @var AppRun $appRun */
        $appRun = parent::patch($name, new Patch($data), $queries);

        return $appRun;
    }

    /**
     * List AppRuns that have not been synced yet
     *
     * Uses label selector to filter for AppRuns where the synced label is not set to 'true'.
     * Uses Kubernetes pagination to avoid loading all items into memory at once.
     *
     * @param int $limit Number of items to fetch per page
     * @return Generator<AppRun>
     */
    public function listNonSyncedAppRuns(int $limit = 100): Generator
    {
        $continueToken = null;

        do {
            $queries = [
                'labelSelector' => 'sandboxes-service.keboola.com/synced!=true',
                'limit' => $limit,
            ];

            if ($continueToken !== null) {
                $queries['continue'] = $continueToken;
            }

            /** @var AppRunList $result */
            $result = $this->list($queries);

            foreach ($result->items ?? [] as $item) {
                yield $item;
            }

            // Get continue token for next page
            $continueToken = $result->metadata?->continue ?? null;
            if ($continueToken === '') {
                $continueToken = null;
            }
        } while ($continueToken !== null);
    }

    /**
     * Mark AppRun as synced by setting the label
     */
    public function markAppRunAsSynced(string $name): AppRun
    {
        $data = [
            'metadata' => [
                'labels' => [
                    'sandboxes-service.keboola.com/synced' => 'true',
                ],
            ],
        ];

        /** @var AppRun */
        return $this->patch($name, new Patch($data));
    }
}
