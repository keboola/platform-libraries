<?php

declare(strict_types=1);

namespace Keboola\SyncActionsClient;

readonly class ActionData
{
    private string $componentId;
    private string $action;
    private array $configData;
    private ?string $tag;
    private ?string $branchId;

    public function __construct(
        string $componentId,
        string $action,
        array $configData = [],
        ?string $tag = null,
        ?string $branchId = null,
    ) {
        $this->componentId = $componentId;
        $this->action = $action;
        $this->configData = $configData;
        $this->tag = $tag;
        $this->branchId = $branchId;
    }

    public function getArray(): array
    {
        $returnData = [
            'componentId' => $this->componentId,
            'action' => $this->action,
            'configData' => $this->configData,
        ];

        if ($this->tag !== null) {
            $returnData['tag'] = $this->tag;
        }
        if ($this->branchId !== null) {
            $returnData['branchId'] = $this->branchId;
        }

        return $returnData;
    }
}
