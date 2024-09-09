<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\RequestMapper;

use CuyZ\Valinor\Mapper\Source\JsonSource;
use Keboola\ApiBundle\RequestMapper\ArgumentResolver;
use Keboola\ApiBundle\RequestMapper\Attribute\RequestPayloadObject;
use Keboola\ApiBundle\RequestMapper\Attribute\RequestQueryObject;
use Keboola\ApiBundle\RequestMapper\DataMapper;
use Keboola\ApiBundle\RequestMapper\Exception\RequestMapperException;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\HttpKernel\ControllerMetadata\ArgumentMetadata;
use Symfony\Component\HttpKernel\ControllerMetadata\ArgumentMetadataFactory;
use Symfony\Component\HttpKernel\Exception\HttpException;
use Symfony\Component\Security\Http\Attribute\CurrentUser;

class ArgumentResolverTest extends TestCase
{
    public function testArgumentWithNoMapperAttribute(): void
    {
        $controller = new class {
            // @phpstan-ignore-next-line no type specified intentionally
            public function __invoke(
                #[CurrentUser] $user,
            ): void {
            }
        };

        $dataMapper = $this->createMock(DataMapper::class);
        $dataMapper->expects(self::never())->method('mapData');

        $resolver = new ArgumentResolver($dataMapper);
        $result = $resolver->resolve(new Request(), $this->createArgumentMetadataForController($controller, 'user'));

        self::assertSame([], [...$result]);
    }

    public static function provideInvalidArguments(): iterable
    {
        yield 'multiple mapper attributes' => [
            'controller' => new class {
                // @phpstan-ignore-next-line no type specified intentionally
                public function __invoke(
                    #[RequestPayloadObject] #[RequestQueryObject] $data,
                ): void {
                }
            },
            'error' => 'Can\'t map argument "data", argument can have only one mapper attribute',
        ];

        yield 'no data type' => [
            'controller' => new class {
                // @phpstan-ignore-next-line no type specified intentionally
                public function __invoke(
                    #[RequestPayloadObject] $data,
                ): void {
                }
            },
            'error' => 'Can\'t map argument "data", argument type not set',
        ];

        yield 'invalid data type' => [
            'controller' => new class {
                public function __invoke(
                    // @phpstan-ignore-next-line invalid type specified intentionally
                    #[RequestPayloadObject] InvalidClass $data,
                ): void {
                }
            },
            'error' =>
                'Can\'t map argument "data", ' .
                'class "Keboola\ApiBundle\Tests\RequestMapper\InvalidClass" does not exist',
        ];

        yield 'unsupported mapper attribute' => [
            'controller' => new class {
                public function __invoke(
                    #[UnsupportedMapperAttribute] RequestData $data,
                ): void {
                }
            },
            'error' =>
                'Can\'t map argument "data", ' .
                'unsupported mapper attribute "Keboola\ApiBundle\Tests\RequestMapper\UnsupportedMapperAttribute"',
        ];
    }

    /** @dataProvider provideInvalidArguments */
    public function testInvalidArguments(object $controller, string $error): void
    {
        $dataMapper = $this->createMock(DataMapper::class);
        $dataMapper->expects(self::never())->method('mapData');

        $resolver = new ArgumentResolver($dataMapper);

        $this->expectException(RequestMapperException::class);
        $this->expectExceptionMessage($error);

        $result = $resolver->resolve(new Request(), $this->createArgumentMetadataForController($controller, 'data'));
        [...$result];
    }

    public static function provideInvalidPayloads(): iterable
    {
        yield 'invalid payload content type' => [
            'request' => new Request(
                server: [
                    'HTTP_CONTENT_TYPE' => 'application/xml',
                ],
            ),
            'errorCode' => Response::HTTP_NOT_ACCEPTABLE,
            'errorMessage' => 'Request content type must be application/json',
        ];

        yield 'invalid JSON payload' => [
            'request' => new Request(
                server: [
                    'HTTP_CONTENT_TYPE' => 'application/json',
                ],
                content: 'invalid json',
            ),
            'errorCode' => Response::HTTP_BAD_REQUEST,
            'errorMessage' => 'Request content is not valid JSON',
        ];
    }

    /** @dataProvider provideInvalidPayloads */
    public function testInvalidPayloadRequest(Request $request, int $errorCode, string $errorMessage): void
    {
        $controller = new class {
            public function __invoke(
                #[RequestPayloadObject] RequestData $data,
            ): void {
            }
        };

        $dataMapper = $this->createMock(DataMapper::class);
        $dataMapper->expects(self::never())->method('mapData');

        $resolver = new ArgumentResolver($dataMapper);

        $error = null;
        try {
            $result = $resolver->resolve($request, $this->createArgumentMetadataForController($controller, 'data'));
            [...$result];
        } catch (HttpException $error) {
            // error is checked below
        }

        self::assertNotNull($error, 'HttpException was not thrown');
        self::assertSame($errorCode, $error->getStatusCode());
        self::assertSame($errorMessage, $error->getMessage());
    }

    public static function provideValidPayloadAttributesTestData(): iterable
    {
        yield 'allow extra keys' => [
            'controller' => new class {
                public function __invoke(
                    #[RequestPayloadObject(allowExtraKeys: true)] RequestData $data,
                ): void {
                }
            },
            'extraKeysEnabled' => true,
        ];

        yield 'disallow extra keys' => [
            'controller' => new class {
                public function __invoke(
                    #[RequestPayloadObject(allowExtraKeys: false)] RequestData $data,
                ): void {
                }
            },
            'extraKeysEnabled' => false,
        ];
    }

    /** @dataProvider provideValidPayloadAttributesTestData */
    public function testValidPayloadRequest(object $controller, bool $extraKeysEnabled): void
    {
        $request = new Request(
            server: [
                'HTTP_CONTENT_TYPE' => 'application/json',
            ],
            content: '{"foo": "bar"}',
        );

        $data = new RequestData(name: 'my name');

        $dataMapper = $this->createMock(DataMapper::class);
        $dataMapper->expects(self::once())
            ->method('mapData')
            ->with(
                RequestData::class,
                new JsonSource('{"foo": "bar"}'),
                'Request contents is not valid',
                Response::HTTP_UNPROCESSABLE_ENTITY,
                false,
                $extraKeysEnabled,
            )
            ->willReturn($data)
        ;

        $resolver = new ArgumentResolver($dataMapper);
        $result = $resolver->resolve($request, $this->createArgumentMetadataForController($controller, 'data'));
        $result = [...$result];

        self::assertCount(1, $result);
        self::assertSame($data, $result[0]);
    }

    public static function provideValidQueryAttributesTestData(): iterable
    {
        yield 'allow extra keys' => [
            'controller' => new class {
                public function __invoke(
                    #[RequestQueryObject(allowExtraKeys: true)] RequestData $data,
                ): void {
                }
            },
            'extraKeysEnabled' => true,
        ];

        yield 'disallow extra keys' => [
            'controller' => new class {
                public function __invoke(
                    #[RequestQueryObject(allowExtraKeys: false)] RequestData $data,
                ): void {
                }
            },
            'extraKeysEnabled' => false,
        ];
    }

    /** @dataProvider provideValidQueryAttributesTestData */
    public function testValidQueryRequest(
        object $controller,
        bool $extraKeysEnabled,
    ): void {
        $request = new Request(
            query: [
                'foo' => 'bar',
            ],
        );

        $data = new RequestData(name: 'my name');

        $dataMapper = $this->createMock(DataMapper::class);
        $dataMapper->expects(self::once())
            ->method('mapData')
            ->with(
                RequestData::class,
                ['foo' => 'bar'],
                'Request query is not valid',
                Response::HTTP_BAD_REQUEST,
                true,
                $extraKeysEnabled,
            )
            ->willReturn($data)
        ;

        $resolver = new ArgumentResolver($dataMapper);
        $result = $resolver->resolve($request, $this->createArgumentMetadataForController($controller, 'data'));
        $result = [...$result];

        self::assertCount(1, $result);
        self::assertSame($data, $result[0]);
    }

    private function createArgumentMetadataForController(object $controller, string $arg): ArgumentMetadata
    {
        $argumentMetadataFactory = new ArgumentMetadataFactory();
        $metadata = $argumentMetadataFactory->createArgumentMetadata($controller);

        foreach ($metadata as $argumentMetadata) {
            if ($argumentMetadata->getName() === $arg) {
                return $argumentMetadata;
            }
        }

        self::fail(sprintf('Controller has no argument "%s"', $arg));
    }
}
