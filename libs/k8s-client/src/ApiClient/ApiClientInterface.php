<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ApiClient;

use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\DeleteOptions;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Status;
use KubernetesRuntime\AbstractModel;

/**
 * @template TList of AbstractModel
 * @template TItem of AbstractModel
 */
interface ApiClientInterface
{
    /**
     * @return TList
     */
    public function list(array $queries = []): AbstractModel;

    /**
     * @return TItem
     */
    public function get(string $name, array $queries = []): AbstractModel;

    /**
     * @param TItem $model
     */
    public function create(AbstractModel $model, array $queries = []): AbstractModel;

    public function delete(string $name, ?DeleteOptions $options = null, array $queries = []): Status;

    public function deleteCollection(?DeleteOptions $options = null, array $queries = []): Status;
}
