<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Exception;

use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Status;
use Throwable;

class KubernetesResponseException extends KubernetesApiFacadeException
{
    private ?Status $status;

    public function __construct(string $message, ?Status $status, int $code = 0, ?Throwable $previous = null)
    {
        parent::__construct($message, $code, $previous);

        $this->status = $status;
    }

    public function getStatus(): ?Status
    {
        return $this->status;
    }
}
