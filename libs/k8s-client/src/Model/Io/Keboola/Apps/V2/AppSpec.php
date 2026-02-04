<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Model\Io\Keboola\Apps\V2;

use KubernetesRuntime\AbstractModel;

/**
 * AppSpec defines the desired state of App
 *
 * @property string|null $appId
 * @property string|null $projectId
 * @property string|null $state
 * @property int|null $replicas
 * @property bool|null $autoRestartEnabled
 * @property string|null $restartRequestedAt
 * @property string|null $runtimeSize
 * @property ContainerSpec|null $containerSpec
 * @property AppFeatures|null $features
 */
class AppSpec extends AbstractModel
{
    /**
     * AppID is the unique identifier of the app
     */
    public string|null $appId = null;

    /**
     * ProjectID is the ID of the project this app belongs to
     */
    public string|null $projectId = null;

    /**
     * State defines whether the app is running or stopped (Running or Stopped)
     */
    public string|null $state = null;

    /**
     * Replicas defines the number of app instances to run (default: 1, minimum: 1)
     */
    public int|null $replicas = null;

    /**
     * AutoRestartEnabled determines if the app should automatically restart on startup probe failures.
     * Default is true. When set to false during deployment of new versions, the PodFailure controller
     * will stop the app if it enters CrashLoopBackOff, preventing infinite restart loops.
     */
    public bool|null $autoRestartEnabled = null;

    /**
     * RestartRequestedAt is a timestamp that signals a restart request.
     * When this field is updated (typically by sandboxes-service during redeployment),
     * the operator detects the change and triggers a rolling restart of the app
     * by updating the Deployment's apps.keboola.com/startedAt annotation.
     * Format: ISO-8601 timestamp (e.g., "2024-01-15T10:30:00Z")
     */
    public string|null $restartRequestedAt = null;

    /**
     * RuntimeSize specifies the size of the runtime (defines resource allocation).
     * The actual resource values are configured via JSON file loaded from
     * RUNTIME_SIZES_CONFIG env variable. Fallback default is taken from
     * RUNTIME_SIZE_DEFAULT env variable if not specified.
     */
    public string|null $runtimeSize = null;

    /**
     * ContainerSpec defines the container specification for the app.
     * In v2, apps run a single container (unlike v1 which supported
     * multiple containers via podSpec).
     */
    public ContainerSpec|null $containerSpec = null;

    /**
     * Features defines optional features that can be enabled for the app
     */
    public AppFeatures|null $features = null;
}
