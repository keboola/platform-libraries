<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Table\Strategy;

use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;
use Keboola\InputMapping\Table\Strategy\WorkspaceLoadPlan;
use Keboola\InputMapping\Table\Strategy\WorkspaceLoadType;
use Keboola\InputMapping\Table\Strategy\WorkspaceTableLoadInstruction;
use PHPUnit\Framework\TestCase;

class WorkspaceLoadPlanTest extends TestCase
{
    public function testConstructorSetsProperties(): void
    {
        $instruction = new WorkspaceTableLoadInstruction(
            WorkspaceLoadType::CLONE,
            $this->createMockTableOptions('table1'),
            null,
        );
        $instructions = [$instruction];
        $preserve = true;

        $plan = new WorkspaceLoadPlan($instructions, $preserve);

        self::assertSame($preserve, $plan->preserve);
    }

    public function testGetCloneInstructionsReturnsOnlyCloneType(): void
    {
        $cloneInstruction1 = new WorkspaceTableLoadInstruction(
            WorkspaceLoadType::CLONE,
            $this->createMockTableOptions('clone1'),
            null,
        );
        $cloneInstruction2 = new WorkspaceTableLoadInstruction(
            WorkspaceLoadType::CLONE,
            $this->createMockTableOptions('clone2'),
            null,
        );
        $copyInstruction = new WorkspaceTableLoadInstruction(
            WorkspaceLoadType::COPY,
            $this->createMockTableOptions('copy1'),
            ['overwrite' => false],
        );
        $viewInstruction = new WorkspaceTableLoadInstruction(
            WorkspaceLoadType::VIEW,
            $this->createMockTableOptions('view1'),
            ['overwrite' => false],
        );

        $plan = new WorkspaceLoadPlan(
            [$cloneInstruction1, $copyInstruction, $cloneInstruction2, $viewInstruction],
            false,
        );

        $cloneInstructions = $plan->getCloneInstructions();

        self::assertCount(2, $cloneInstructions);
        self::assertSame($cloneInstruction1, $cloneInstructions[0]);
        self::assertSame($cloneInstruction2, $cloneInstructions[2]);
    }

    public function testGetCopyInstructionsReturnsCopyAndViewTypes(): void
    {
        $cloneInstruction = new WorkspaceTableLoadInstruction(
            WorkspaceLoadType::CLONE,
            $this->createMockTableOptions('clone1'),
            null,
        );
        $copyInstruction1 = new WorkspaceTableLoadInstruction(
            WorkspaceLoadType::COPY,
            $this->createMockTableOptions('copy1'),
            ['overwrite' => false],
        );
        $copyInstruction2 = new WorkspaceTableLoadInstruction(
            WorkspaceLoadType::COPY,
            $this->createMockTableOptions('copy2'),
            ['overwrite' => true],
        );
        $viewInstruction = new WorkspaceTableLoadInstruction(
            WorkspaceLoadType::VIEW,
            $this->createMockTableOptions('view1'),
            ['overwrite' => false],
        );

        $plan = new WorkspaceLoadPlan(
            [$cloneInstruction, $copyInstruction1, $viewInstruction, $copyInstruction2],
            false,
        );

        $copyInstructions = $plan->getCopyInstructions();

        self::assertCount(3, $copyInstructions);
        self::assertSame($copyInstruction1, $copyInstructions[1]);
        self::assertSame($viewInstruction, $copyInstructions[2]);
        self::assertSame($copyInstruction2, $copyInstructions[3]);
    }

    public function testHasCloneInstructionsReturnsTrueWhenCloneInstructionsExist(): void
    {
        $cloneInstruction = new WorkspaceTableLoadInstruction(
            WorkspaceLoadType::CLONE,
            $this->createMockTableOptions('clone1'),
            null,
        );
        $copyInstruction = new WorkspaceTableLoadInstruction(
            WorkspaceLoadType::COPY,
            $this->createMockTableOptions('copy1'),
            ['overwrite' => false],
        );

        $plan = new WorkspaceLoadPlan([$cloneInstruction, $copyInstruction], false);

        self::assertTrue($plan->hasCloneInstructions());
    }

    public function testHasCloneInstructionsReturnsFalseWhenNoCloneInstructionsExist(): void
    {
        $copyInstruction = new WorkspaceTableLoadInstruction(
            WorkspaceLoadType::COPY,
            $this->createMockTableOptions('copy1'),
            ['overwrite' => false],
        );
        $viewInstruction = new WorkspaceTableLoadInstruction(
            WorkspaceLoadType::VIEW,
            $this->createMockTableOptions('view1'),
            ['overwrite' => false],
        );

        $plan = new WorkspaceLoadPlan([$copyInstruction, $viewInstruction], false);

        self::assertFalse($plan->hasCloneInstructions());
    }

    public function testHasCopyInstructionsReturnsTrueWhenCopyOrViewInstructionsExist(): void
    {
        $cloneInstruction = new WorkspaceTableLoadInstruction(
            WorkspaceLoadType::CLONE,
            $this->createMockTableOptions('clone1'),
            null,
        );
        $copyInstruction = new WorkspaceTableLoadInstruction(
            WorkspaceLoadType::COPY,
            $this->createMockTableOptions('copy1'),
            ['overwrite' => false],
        );

        $planWithCopy = new WorkspaceLoadPlan([$cloneInstruction, $copyInstruction], false);
        self::assertTrue($planWithCopy->hasCopyInstructions());

        $viewInstruction = new WorkspaceTableLoadInstruction(
            WorkspaceLoadType::VIEW,
            $this->createMockTableOptions('view1'),
            ['overwrite' => false],
        );

        $planWithView = new WorkspaceLoadPlan([$cloneInstruction, $viewInstruction], false);
        self::assertTrue($planWithView->hasCopyInstructions());
    }

    public function testHasCopyInstructionsReturnsFalseWhenOnlyCloneInstructionsExist(): void
    {
        $cloneInstruction1 = new WorkspaceTableLoadInstruction(
            WorkspaceLoadType::CLONE,
            $this->createMockTableOptions('clone1'),
            null,
        );
        $cloneInstruction2 = new WorkspaceTableLoadInstruction(
            WorkspaceLoadType::CLONE,
            $this->createMockTableOptions('clone2'),
            null,
        );

        $plan = new WorkspaceLoadPlan([$cloneInstruction1, $cloneInstruction2], false);

        self::assertFalse($plan->hasCopyInstructions());
    }

    public function testEmptyPlan(): void
    {
        $plan = new WorkspaceLoadPlan([], true);

        self::assertEmpty($plan->getCloneInstructions());
        self::assertEmpty($plan->getCopyInstructions());
        self::assertFalse($plan->hasCloneInstructions());
        self::assertFalse($plan->hasCopyInstructions());
        self::assertTrue($plan->preserve);
    }

    private function createMockTableOptions(string $source): RewrittenInputTableOptions
    {
        return new RewrittenInputTableOptions(
            ['source' => $source, 'destination' => $source],
            $source,
            123,
            [
                'id' => $source,
                'bucket' => ['backend' => 'snowflake'],
                'isAlias' => false,
            ],
        );
    }
}
