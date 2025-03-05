<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\RequestMapper;

use CuyZ\Valinor\MapperBuilder;
use Keboola\ApiBundle\RequestMapper\DataMapper;
use Keboola\ApiBundle\RequestMapper\Exception\InvalidPayloadException;
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
}
