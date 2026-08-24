<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Table;

use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Result;
use PHPUnit\Framework\TestCase;

class ResultTest extends TestCase
{
    public function testInputTableStateListIsReadableWithoutBeingSet(): void
    {
        $result = new Result();

        self::assertSame([], $result->getInputTableStateList()->jsonSerialize());
    }

    public function testSetInputTableStateListReplacesTheDefault(): void
    {
        $result = new Result();
        $result->setInputTableStateList(new InputTableStateList([
            [
                'source' => 'in.c-bucket.table',
                'lastImportDate' => '2026-08-20T12:00:00+0200',
            ],
        ]));

        self::assertSame(
            [
                [
                    'source' => 'in.c-bucket.table',
                    'lastImportDate' => '2026-08-20T12:00:00+0200',
                ],
            ],
            $result->getInputTableStateList()->jsonSerialize(),
        );
    }
}
