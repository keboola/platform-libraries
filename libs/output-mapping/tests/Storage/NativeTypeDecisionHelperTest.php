<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Storage;

use Generator;
use Keboola\OutputMapping\Storage\NativeTypeDecisionHelper;
use Keboola\StorageApiBranch\StorageApiToken;
use PHPUnit\Framework\TestCase;

class NativeTypeDecisionHelperTest extends TestCase
{
    public function shouldEnforceBaseTypesDataProvider(): Generator
    {
        yield 'snowflake with feature' => [
            'backend' => 'snowflake',
            'hasFeature' => true,
            'expectedResult' => false,
        ];
        yield 'snowflake without feature' => [
            'backend' => 'snowflake',
            'hasFeature' => false,
            'expectedResult' => false,
        ];
        yield 'bigquery with feature' => [
            'backend' => 'bigquery',
            'hasFeature' => true,
            'expectedResult' => false,
        ];
        yield 'bigquerye without feature' => [
            'backend' => 'bigquery',
            'hasFeature' => false,
            'expectedResult' => true,
        ];
    }

    /**
     * @dataProvider shouldEnforceBaseTypesDataProvider
     */
    public function testShouldEnforceBaseTypes(
        string $backend,
        bool $hasFeature,
        bool $expectedResult,
    ): void {
        $tokenMock = $this->createMock(StorageApiToken::class);
        $tokenMock->expects(self::once())
            ->method('hasFeature')
            ->with('bigquery-native-types')
            ->willReturn($hasFeature)
        ;

        self::assertSame($expectedResult, NativeTypeDecisionHelper::shouldEnforceBaseTypes($tokenMock, $backend));
    }
}
