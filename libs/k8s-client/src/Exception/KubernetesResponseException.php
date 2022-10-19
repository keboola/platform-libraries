<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Exception;

use KubernetesRuntime\AbstractModel;
use Throwable;

class KubernetesResponseException extends KubernetesApiFacadeException
{
    private ?AbstractModel $result;

    public function __construct(string $message, ?AbstractModel $result, int $code = 0, ?Throwable $previous = null)
    {
        parent::__construct($message, $code, $previous);

        $this->result = $result;
    }

    public function getResult(): ?AbstractModel
    {
        return $this->result;
    }
}
