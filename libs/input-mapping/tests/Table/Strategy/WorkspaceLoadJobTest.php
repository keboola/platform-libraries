<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Table\Strategy;

use Generator;
use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;
use Keboola\InputMapping\Table\Strategy\WorkspaceLoadJob;
use Keboola\InputMapping\Table\Strategy\WorkspaceLoadType;
use PHPUnit\Framework\TestCase;

class WorkspaceLoadJobTest extends TestCase
{
    /**
     * @dataProvider validJobTypeProvider
     */
    public function testConstructorWithValidJobTypes(WorkspaceLoadType $jobType): void
    {
        $this->expectNotToPerformAssertions();

        $tableOptions = $this->createMock(RewrittenInputTableOptions::class);
        new WorkspaceLoadJob('123', $jobType, [$tableOptions]);
    }

    /**
     * @dataProvider invalidJobTypeProvider
     */
    public function testConstructorThrowsExceptionForInvalidJobTypes(WorkspaceLoadType $jobType): void
    {
        $this->expectException(InputOperationException::class);
        $this->expectExceptionMessage(
            sprintf('Invalid job type "%s". Only CLONE and COPY are allowed for jobs.', $jobType->value),
        );

        $tableOptions = $this->createMock(RewrittenInputTableOptions::class);
        new WorkspaceLoadJob('789', $jobType, [$tableOptions]);
    }

    public static function validJobTypeProvider(): Generator
    {
        yield 'CLONE job type' => [WorkspaceLoadType::CLONE];
        yield 'COPY job type' => [WorkspaceLoadType::COPY];
    }

    public static function invalidJobTypeProvider(): Generator
    {
        yield 'VIEW job type' => [WorkspaceLoadType::VIEW];
    }
}
