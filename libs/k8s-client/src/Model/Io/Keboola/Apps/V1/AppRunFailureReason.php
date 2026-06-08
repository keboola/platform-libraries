<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V1;

use KubernetesRuntime\AbstractModel;

/**
 * AppRunFailureReason is the user-facing cause of a Failed run. Only set when state=Failed.
 *
 * Populated by the operator at the moment the run transitions to Failed; never updated
 * afterwards. Downstream consumers SHOULD treat absence as an unclassified failure
 * (legacy AppRuns predating this field).
 */
class AppRunFailureReason extends AbstractModel
{
    /**
     * Reason is a short, stable, machine-readable identifier (UI i18n key).
     *
     * @var string
     */
    public $reason = null;

    /**
     * Message is a human-readable description of the failure with no internal
     * resource names or cluster-internal details.
     *
     * @var string
     */
    public $message = null;
}
