<?php

declare(strict_types=1);

namespace Keboola\SandboxesServiceApiClient\Apps;

use Keboola\SandboxesServiceApiClient\Exception\ClientException;

class App
{
    protected const REQUIRED_PROPERTIES = [
        'id',
        'name',
        'projectId',
        'componentId',
        'configId',
        'configVersion',
        'state',
        'desiredState',
        'provisioningStrategy',
    ];

    public const STATE_CREATED = 'created';
    public const STATE_STARTING = 'starting';
    public const STATE_RUNNING = 'running';
    public const STATE_STOPPING = 'stopping';
    public const STATE_STOPPED = 'stopped';
    public const STATE_DELETING = 'deleting';
    public const STATE_DELETED = 'deleted';

    public const DESIRED_STATE_RUNNING = 'running';
    public const DESIRED_STATE_STOPPED = 'stopped';
    public const DESIRED_STATE_DELETED = 'deleted';

    public const PROVISIONING_STRATEGY_JOB_QUEUE = 'jobQueue';
    public const PROVISIONING_STRATEGY_OPERATOR = 'operator';

    /** @var array<string> */
    public const VALID_STATES = [
        self::STATE_CREATED,
        self::STATE_STARTING,
        self::STATE_RUNNING,
        self::STATE_STOPPING,
        self::STATE_STOPPED,
        self::STATE_DELETING,
        self::STATE_DELETED,
    ];

    /** @var array<string> */
    public const VALID_DESIRED_STATES = [
        self::DESIRED_STATE_RUNNING,
        self::DESIRED_STATE_STOPPED,
        self::DESIRED_STATE_DELETED,
    ];

    /** @var array<string> */
    public const VALID_PROVISIONING_STRATEGIES = [
        self::PROVISIONING_STRATEGY_JOB_QUEUE,
        self::PROVISIONING_STRATEGY_OPERATOR,
    ];

    private string $id;
    private string $name;
    private string $projectId;
    private string $componentId;
    private ?string $branchId = null;
    private string $configId;
    private string $configVersion;
    private string $state;
    private string $desiredState;
    private ?string $lastRequestTimestamp = null;
    private ?string $url = null;
    private int $autoSuspendAfterSeconds;
    private string $provisioningStrategy;

    public static function fromArray(array $in): self
    {
        foreach (self::REQUIRED_PROPERTIES as $property) {
            if (!isset($in[$property])) {
                throw new ClientException("Property $property is missing from API response");
            }
        }

        $app = new self();
        $app->setId((string) $in['id']);
        $app->setName((string) $in['name']);
        $app->setProjectId((string) $in['projectId']);
        $app->setComponentId((string) $in['componentId']);
        $app->setBranchId(isset($in['branchId']) ? $in['branchId'] : null);
        $app->setConfigId((string) $in['configId']);
        $app->setConfigVersion((string) $in['configVersion']);
        $app->setState((string) $in['state']);
        $app->setDesiredState((string) $in['desiredState']);
        $app->setLastRequestTimestamp($in['lastRequestTimestamp'] ?? null);
        $app->setUrl($in['url'] ?? null);
        $app->setAutoSuspendAfterSeconds((int) ($in['autoSuspendAfterSeconds'] ?? 0));
        $app->setProvisioningStrategy((string) $in['provisioningStrategy']);

        return $app;
    }

    public function toArray(): array
    {
        return [
            'id' => $this->id,
            'name' => $this->name,
            'projectId' => $this->projectId,
            'componentId' => $this->componentId,
            'branchId' => $this->branchId,
            'configId' => $this->configId,
            'configVersion' => $this->configVersion,
            'state' => $this->state,
            'desiredState' => $this->desiredState,
            'lastRequestTimestamp' => $this->lastRequestTimestamp,
            'url' => $this->url,
            'autoSuspendAfterSeconds' => $this->autoSuspendAfterSeconds,
            'provisioningStrategy' => $this->provisioningStrategy,
        ];
    }

    public function getId(): string
    {
        return $this->id;
    }

    public function setId(string $id): self
    {
        $this->id = $id;
        return $this;
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function setName(string $name): self
    {
        $this->name = $name;
        return $this;
    }

    public function getProjectId(): string
    {
        return $this->projectId;
    }

    public function setProjectId(string $projectId): self
    {
        $this->projectId = $projectId;
        return $this;
    }

    public function getComponentId(): string
    {
        return $this->componentId;
    }

    public function setComponentId(string $componentId): self
    {
        $this->componentId = $componentId;
        return $this;
    }

    public function getBranchId(): ?string
    {
        return $this->branchId;
    }

    public function setBranchId(?string $branchId): self
    {
        $this->branchId = $branchId;
        return $this;
    }

    public function getConfigId(): string
    {
        return $this->configId;
    }

    public function setConfigId(string $configId): self
    {
        $this->configId = $configId;
        return $this;
    }

    public function getConfigVersion(): string
    {
        return $this->configVersion;
    }

    public function setConfigVersion(string $configVersion): self
    {
        $this->configVersion = $configVersion;
        return $this;
    }

    public function getState(): string
    {
        return $this->state;
    }

    public function setState(string $state): self
    {
        if (!in_array($state, self::VALID_STATES, true)) {
            throw new ClientException(
                sprintf('Invalid state "%s". Valid states are: %s', $state, implode(', ', self::VALID_STATES)),
            );
        }
        $this->state = $state;
        return $this;
    }

    public function getDesiredState(): string
    {
        return $this->desiredState;
    }

    public function setDesiredState(string $desiredState): self
    {
        if (!in_array($desiredState, self::VALID_DESIRED_STATES, true)) {
            throw new ClientException(
                sprintf(
                    'Invalid desired state "%s". Valid desired states are: %s',
                    $desiredState,
                    implode(', ', self::VALID_DESIRED_STATES),
                ),
            );
        }
        $this->desiredState = $desiredState;
        return $this;
    }

    public function getLastRequestTimestamp(): ?string
    {
        return $this->lastRequestTimestamp;
    }

    public function setLastRequestTimestamp(?string $lastRequestTimestamp): self
    {
        $this->lastRequestTimestamp = $lastRequestTimestamp;
        return $this;
    }

    public function getUrl(): ?string
    {
        return $this->url;
    }

    public function setUrl(?string $url): self
    {
        $this->url = $url;
        return $this;
    }

    public function getAutoSuspendAfterSeconds(): int
    {
        return $this->autoSuspendAfterSeconds;
    }

    public function setAutoSuspendAfterSeconds(int $autoSuspendAfterSeconds): self
    {
        $this->autoSuspendAfterSeconds = $autoSuspendAfterSeconds;
        return $this;
    }

    public function getProvisioningStrategy(): string
    {
        return $this->provisioningStrategy;
    }

    public function setProvisioningStrategy(string $provisioningStrategy): self
    {
        if (!in_array($provisioningStrategy, self::VALID_PROVISIONING_STRATEGIES, true)) {
            throw new ClientException(
                sprintf(
                    'Invalid provisioning strategy "%s". Valid strategies are: %s',
                    $provisioningStrategy,
                    implode(', ', self::VALID_PROVISIONING_STRATEGIES),
                ),
            );
        }
        $this->provisioningStrategy = $provisioningStrategy;
        return $this;
    }
}
