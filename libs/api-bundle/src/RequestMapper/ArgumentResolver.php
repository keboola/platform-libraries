<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\RequestMapper;

use CuyZ\Valinor\Mapper\Source\Exception\InvalidSource;
use CuyZ\Valinor\Mapper\Source\JsonSource;
use Keboola\ApiBundle\RequestMapper\Attribute\RequestMapperAttributeInterface;
use Keboola\ApiBundle\RequestMapper\Attribute\RequestPayloadObject;
use Keboola\ApiBundle\RequestMapper\Attribute\RequestQueryObject;
use Keboola\ApiBundle\RequestMapper\Exception\RequestMapperException;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\HttpKernel\Controller\ValueResolverInterface;
use Symfony\Component\HttpKernel\ControllerMetadata\ArgumentMetadata;
use Symfony\Component\HttpKernel\Exception\HttpException;

class ArgumentResolver implements ValueResolverInterface
{
    public function __construct(
        private readonly DataMapper $dataMapper,
    ) {
    }

    public function resolve(Request $request, ArgumentMetadata $argument): iterable
    {
        $mapperAttributes = $argument->getAttributesOfType(
            RequestMapperAttributeInterface::class,
            ArgumentMetadata::IS_INSTANCEOF,
        );

        if (count($mapperAttributes) === 0) {
            return [];
        }

        if (count($mapperAttributes) > 1) {
            throw new RequestMapperException(sprintf(
                'Can\'t map argument "%s", argument can have only one mapper attribute',
                $argument->getName(),
            ));
        }
        $attribute = reset($mapperAttributes);

        $argumentType = $argument->getType();
        if ($argumentType === null || $argumentType === '') {
            throw new RequestMapperException(sprintf(
                'Can\'t map argument "%s", argument type not set',
                $argument->getName(),
            ));
        }

        if (!class_exists($argumentType)) {
            throw new RequestMapperException(sprintf(
                'Can\'t map argument "%s", class "%s" does not exist',
                $argument->getName(),
                $argumentType,
            ));
        }

        if ($attribute instanceof RequestPayloadObject) {
            yield $this->resolveRequestPayloadObject($request, $argumentType, $attribute);
        } elseif ($attribute instanceof RequestQueryObject) {
            yield $this->resolveRequestQueryObject($request, $argumentType, $attribute);
        } else {
            throw new RequestMapperException(sprintf(
                'Can\'t map argument "%s", unsupported mapper attribute "%s"',
                $argument->getName(),
                get_class($attribute),
            ));
        }
    }

    /**
     * @template T of object
     * @param class-string<T> $argumentType
     * @return T
     */
    private function resolveRequestPayloadObject(
        Request $request,
        string $argumentType,
        RequestPayloadObject $attribute,
    ): object {
        if ($request->getContentTypeFormat() !== 'json') {
            throw new HttpException(Response::HTTP_NOT_ACCEPTABLE, 'Request content type must be application/json');
        }

        try {
            $data = new JsonSource($request->getContent());
        } catch (InvalidSource $e) {
            throw new HttpException(Response::HTTP_BAD_REQUEST, 'Request content is not valid JSON', $e);
        }

        return $this->dataMapper->mapData(
            $argumentType,
            $data,
            'Request contents is not valid',
            Response::HTTP_UNPROCESSABLE_ENTITY,
            enableExtraKeys: $attribute->allowExtraKeys,
        );
    }

    /**
     * @template T of object
     * @param class-string<T> $argumentType
     * @return T
     */
    private function resolveRequestQueryObject(
        Request $request,
        string $argumentType,
        RequestQueryObject $attribute,
    ): object {
        $data = $request->query->all();

        return $this->dataMapper->mapData(
            $argumentType,
            $data,
            'Request query is not valid',
            Response::HTTP_BAD_REQUEST,
            enableFlexibleCasting: true,
            enableExtraKeys: $attribute->allowExtraKeys,
        );
    }
}
