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
}
