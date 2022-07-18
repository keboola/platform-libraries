<?php

namespace Keboola\InputMapping\Tests\Staging;

use Keboola\InputMapping\Exception\StagingException;
use Keboola\InputMapping\Staging\Scope;
use PHPUnit\Framework\TestCase;

class ScopeTest extends TestCase
{
    public function testCreate()
    {
        $scope = new Scope([Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_DATA, Scope::TABLE_METADATA]);
        self::assertSame([Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_DATA, Scope::TABLE_METADATA], $scope->getScopeTypes());
        $scope = new Scope([Scope::FILE_DATA]);
        self::assertSame([Scope::FILE_DATA], $scope->getScopeTypes());
        $scope = new Scope([Scope::TABLE_METADATA]);
        self::assertSame([Scope::TABLE_METADATA], $scope->getScopeTypes());
        $scope = new Scope([]);
        self::assertSame([], $scope->getScopeTypes());
    }

    public function testCreateInvalid()
    {
        self::expectException(StagingException::class);
        self::expectExceptionMessage('Unknown scope types "boo".');
        new Scope(['boo']);
    }
}
