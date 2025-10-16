<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Strategy;

final class WorkspaceLoadPlan
{
    /**
     * @param WorkspaceTableLoadInstruction[] $instructions
     */
    public function __construct(
        private readonly array $instructions,
        public readonly bool $preserve,
    ) {
    }

    /**
     * @return WorkspaceTableLoadInstruction[]
     */
    public function getCloneInstructions(): array
    {
        return array_filter(
            $this->instructions,
            fn(WorkspaceTableLoadInstruction $instruction) => $instruction->loadType === WorkspaceLoadType::CLONE,
        );
    }

    /**
     * @return WorkspaceTableLoadInstruction[]
     */
    public function getCopyInstructions(): array
    {
        return array_filter(
            $this->instructions,
            fn(WorkspaceTableLoadInstruction $instruction) => in_array(
                $instruction->loadType,
                [WorkspaceLoadType::COPY, WorkspaceLoadType::VIEW],
                true,
            ),
        );
    }

    public function hasCloneInstructions(): bool
    {
        return !empty($this->getCloneInstructions());
    }

    public function hasCopyInstructions(): bool
    {
        return !empty($this->getCopyInstructions());
    }

    /**
     * @return WorkspaceTableLoadInstruction[]
     */
    public function getAllInstructions(): array
    {
        return $this->instructions;
    }

    public function hasInstructions(): bool
    {
        return !empty($this->instructions);
    }

    /**
     * @return array{clone: int, copy: int, view: int, total: int}
     */
    public function getStatistics(): array
    {
        $stats = ['clone' => 0, 'copy' => 0, 'view' => 0];

        foreach ($this->instructions as $instruction) {
            $type = strtolower($instruction->loadType->value);
            if (isset($stats[$type])) {
                $stats[$type]++;
            }
        }

        $stats['total'] = count($this->instructions);
        return $stats;
    }
}
