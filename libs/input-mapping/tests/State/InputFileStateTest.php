<?php

namespace Keboola\InputMapping\Tests\State;

use Keboola\InputMapping\State\InputFileState;
use PHPUnit\Framework\TestCase;

class InputFileStateTest extends TestCase
{
    public function testGetTags()
    {
        $configuration = [
            'tags' => [
                [
                    'name' => 'test',
                    'match' => 'include',
                ],
            ],
            'lastImportId' => '12345',
        ];
        $state = new InputFileState($configuration);
        self::assertEquals([['name' => 'test', 'match' => 'include']], $state->getTags());
    }

    public function testGetLastImportId()
    {
        $configuration = [
            'tags' => [
                [
                    'name' => 'test',
                    'match' => 'include',
                ],
            ],
            'lastImportId' => '12345',
        ];
        $state = new InputFileState($configuration);
        self::assertEquals('12345', $state->getLastImportId());
    }

    public function testJsonSerialize()
    {
        $configuration = [
            'tags' => [
                [
                    'name' => 'test',
                    'match' => 'include',
                ],
            ],
            'lastImportId' => '12345',
        ];
        $state = new InputFileState($configuration);
        self::assertEquals($configuration, $state->jsonSerialize());
    }
}
