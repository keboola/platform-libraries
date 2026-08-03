<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\RequestMapper;

use CuyZ\Valinor\MapperBuilder;
use Keboola\ApiBundle\RequestMapper\DataMapper;
use Keboola\ApiBundle\RequestMapper\Exception\InvalidPayloadException;
use Keboola\ApiBundle\RequestMapper\Exception\RequestMapperException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Validator\Validation;

class DataMapperTest extends TestCase
{
    public static function provideValidData(): iterable
    {
        yield 'valid data' => [
            'data' => [
                'name' => 'my name',
                'config' => null,
            ],
            'enableFlexibleCasting' => false,
            'enableExtraKeys' => false,
            'result' => new RequestData(
                name: 'my name',
                config: null,
            ),
        ];

        yield 'enabled extra keys' => [
            'data' => [
                'name' => 'my name',
                'foo' => 'bar',
                'config' => null,
            ],
            'enableFlexibleCasting' => false,
            'enableExtraKeys' => true,
            'result' => new RequestData(
                name: 'my name',
                config: null,
            ),
        ];

        yield 'enabled casting' => [
            'data' => [
                'name' => 1,
                'config' => null,
            ],
            'enableFlexibleCasting' => true,
            'enableExtraKeys' => false,
            'result' => new RequestData(
                name: '1',
                config: null,
            ),
        ];

        yield 'valid array' => [
            'data' => [
                'name' => 'array',
                'config' => ['foo' => 'bar'],
            ],
            'enableFlexibleCasting' => false,
            'enableExtraKeys' => false,
            'result' => new RequestData(
                name: 'array',
                config: ['foo' => 'bar'],
            ),
        ];

        yield 'empty array' => [
            'data' => [
                'name' => 'array',
                'config' => [],
            ],
            'enableFlexibleCasting' => false,
            'enableExtraKeys' => false,
            'result' => new RequestData(
                name: 'array',
                config: [],
            ),
        ];
    }

    #[DataProvider('provideValidData')]
    public function testMapValidData(
        array $data,
        bool $enableFlexibleCasting,
        bool $enableExtraKeys,
        object $result,
    ): void {
        $mapper = new DataMapper(
            new MapperBuilder(),
            Validation::createValidatorBuilder()
                ->enableAttributeMapping()
                ->getValidator(),
        );

        $actualResult = $mapper->mapData(
            RequestData::class,
            $data,
            'Invalid data',
            400,
            $enableFlexibleCasting,
            $enableExtraKeys,
        );

        self::assertEquals($result, $actualResult);
    }

    public static function provideErroneousData(): iterable
    {
        yield 'failing type mapping when casting disabled' => [
            'data' => [
                'name' => 1,
                'config' => null,
            ],
            'enableFlexibleCasting' => false,
            'enableExtraKeys' => false,
            'errorContext' => [
                [
                    'path' => 'name',
                    'message' => 'Value 1 is not a valid string.',
                ],
            ],
        ];

        yield 'failing type mapping when casting enabled' => [
            'data' => [
                'name' => [],
                'config' => [],
            ],
            'enableFlexibleCasting' => true,
            'enableExtraKeys' => false,
            'errorContext' => [
                [
                    'path' => 'name',
                    'message' => 'Value array (empty) is not a valid string.',
                ],
            ],
        ];

        yield 'failing mapping with extra keys' => [
            'data' => [
                'name' => 'my name',
                'foo' => 'bar',
                'config' => [],
            ],
            'enableFlexibleCasting' => false,
            'enableExtraKeys' => false,
            'errorContext' => [
                [
                    'path' => '*root*',
                    'message' => 'Unexpected key(s) `foo`, expected `name`, `config`.',
                ],
            ],
        ];

        yield 'failing validation' => [
            'data' => [
                'name' => 'my name longer than allowed',
                'config' => [],
            ],
            'enableFlexibleCasting' => false,
            'enableExtraKeys' => false,
            'errorContext' => [
                [
                    'path' => 'name',
                    'message' => 'This value is too long. It should have 16 characters or less.',
                ],
            ],
        ];
    }

    #[DataProvider('provideErroneousData')]
    public function testMapErroneousData(
        array $data,
        bool $enableFlexibleCasting,
        bool $enableExtraKeys,
        array $errorContext,
    ): void {
        $mapper = new DataMapper(
            new MapperBuilder(),
            Validation::createValidatorBuilder()
                ->enableAttributeMapping()
                ->getValidator(),
        );

        $error = null;
        try {
            $mapper->mapData(
                RequestData::class,
                $data,
                'Invalid data',
                400,
                $enableFlexibleCasting,
                $enableExtraKeys,
            );
        } catch (InvalidPayloadException $error) {
        }

        self::assertNotNull($error, 'Exception was not thrown');
        self::assertSame('Invalid data', $error->getMessage());
        self::assertSame(400, $error->getCode());
        self::assertSame($errorContext, $error->getContext());
    }

    private function createMapper(): DataMapper
    {
        return new DataMapper(
            new MapperBuilder(),
            Validation::createValidatorBuilder()
                ->enableAttributeMapping()
                ->getValidator(),
        );
    }

    /**
     * @return array{0: ?KeyMappedRequestData, 1: ?InvalidPayloadException}
     */
    private function mapKeyMapped(array $data): array
    {
        try {
            return [$this->createMapper()->mapData(KeyMappedRequestData::class, $data, 'Invalid data', 400), null];
        } catch (InvalidPayloadException $e) {
            return [null, $e];
        }
    }

    public function testDuplicateRequestKeyIsRejected(): void
    {
        $this->expectException(RequestMapperException::class);
        $this->expectExceptionMessage(
            'Class "Keboola\ApiBundle\Tests\RequestMapper\DuplicateKeyRequestData" maps request key ' .
            '"#data" to both "first" and "second"',
        );

        $this->createMapper()->mapData(DuplicateKeyRequestData::class, ['#data' => 'x'], 'Invalid data', 400);
    }

    public function testRequestKeyOnNestedObjectIsRejected(): void
    {
        // The key map is only applied to top-level payload keys. Declaring it deeper used to be
        // silently ignored — which inverted the guarantee, since the bare property name then worked
        // and the declared wire key did not. Fail loudly instead.
        $this->expectException(RequestMapperException::class);
        $this->expectExceptionMessage(
            'Class "Keboola\ApiBundle\Tests\RequestMapper\NestedOuterWithKeyedInner" declares ' .
            '#[RequestKey] on nested payload object ' .
            '"Keboola\ApiBundle\Tests\RequestMapper\NestedInnerWithKey"; ' .
            'only top-level payload properties are supported',
        );

        $this->createMapper()->mapData(
            NestedOuterWithKeyedInner::class,
            ['name' => 'n', 'inner' => ['#secret' => 's']],
            'Invalid data',
            400,
        );
    }

    public function testNestedObjectWithoutRequestKeyStillMaps(): void
    {
        $result = $this->createMapper()->mapData(
            NestedOuterPlain::class,
            ['name' => 'n', 'inner' => ['secret' => 's'], 'union' => 3],
            'Invalid data',
            400,
        );

        self::assertSame('n', $result->name);
        self::assertSame('s', $result->inner->secret);
    }

    public function testKeyMapDeclaredOnPropertyIsApplied(): void
    {
        [$result, $error] = $this->mapKeyMapped(['#data' => 'secret']);

        self::assertNull($error);
        self::assertNotNull($result);
        self::assertSame('secret', $result->data);
    }

    public function testKeyMapRejectsBareTargetKey(): void
    {
        // `data` is a rename *target*, not a wire key — it must not populate the property.
        [$result, $error] = $this->mapKeyMapped(['data' => 'secret']);

        self::assertNull($result);
        self::assertNotNull($error);
        self::assertSame([['path' => '#data', 'message' => 'Cannot be empty and must be filled with a value ' .
            'matching type `string`.']], $error->getContext());
    }

    public static function provideBothKeysOrderings(): iterable
    {
        yield 'wire key first' => ['data' => ['#data' => 'from-wire', 'data' => 'bare']];
        yield 'bare key first' => ['data' => ['data' => 'bare', '#data' => 'from-wire']];
    }

    #[DataProvider('provideBothKeysOrderings')]
    public function testKeyMapIsOrderIndependent(array $data): void
    {
        [$result, $error] = $this->mapKeyMapped($data);

        self::assertNull($error);
        self::assertNotNull($result);
        self::assertSame('from-wire', $result->data);
    }

    public function testKeyMapInvertsMappingErrorPaths(): void
    {
        [, $error] = $this->mapKeyMapped([]);

        self::assertNotNull($error);
        self::assertSame(['#data'], array_column($error->getContext(), 'path'));
    }

    public function testKeyMapInvertsValidationErrorPaths(): void
    {
        [, $error] = $this->mapKeyMapped(['#data' => 'a value longer than allowed']);

        self::assertNotNull($error);
        self::assertSame(
            [['path' => '#data', 'message' => 'This value is too long. It should have 16 characters or less.']],
            $error->getContext(),
        );
    }

    public function testUnmappedKeysAreLeftUntouched(): void
    {
        [$result, $error] = $this->mapKeyMapped(['#data' => 'secret', 'name' => 'my name']);

        self::assertNull($error);
        self::assertNotNull($result);
        self::assertSame('my name', $result->name);
    }
}
