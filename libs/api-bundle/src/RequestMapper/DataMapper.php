<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\RequestMapper;

use CuyZ\Valinor\Mapper\MappingError;
use CuyZ\Valinor\Mapper\Tree\Message\Messages;
use CuyZ\Valinor\Mapper\Tree\Message\NodeMessage;
use CuyZ\Valinor\MapperBuilder;
use Keboola\ApiBundle\RequestMapper\Exception\InvalidPayloadException;
use Symfony\Component\DependencyInjection\Attribute\Autowire;
use Symfony\Component\Validator\ConstraintViolationInterface;
use Symfony\Component\Validator\Validator\ValidatorInterface;

class DataMapper
{
    public function __construct(
        #[Autowire(service: 'valinor.mapper_builder')]
        private readonly MapperBuilder $mapperBuilder,
        private readonly ValidatorInterface $validator,
    ) {
    }

    /**
     * @template T of object
     * @param class-string<T> $objectType
     * @return T
     */
    public function mapData(
        string $objectType,
        mixed $data,
        string $errorMessage,
        int $errorCode,
        bool $enableFlexibleCasting = false,
        bool $enableExtraKeys = false,
    ): object {
        $mapperBuilder = $this->mapperBuilder;
        if ($enableFlexibleCasting) {
            $mapperBuilder = $mapperBuilder->enableFlexibleCasting();
        }
        if ($enableExtraKeys) {
            $mapperBuilder = $mapperBuilder->allowSuperfluousKeys();
        }

        try {
            $object = $mapperBuilder->mapper()->map($objectType, $data);
        } catch (MappingError $e) {
            throw new InvalidPayloadException(
                $errorMessage,
                $errorCode,
                array_map(
                    fn(NodeMessage $message) => [
                        'path' => $message->node()->path(),
                        'message' => (string) $message,
                    ],
                    [...Messages::flattenFromNode($e->node())->errors()],
                ),
                $e,
            );
        }

        $violations = $this->validator->validate($object);
        if (count($violations) > 0) {
            throw new InvalidPayloadException($errorMessage, $errorCode, array_map(
                fn(ConstraintViolationInterface $error) => [
                    'path' => $error->getPropertyPath(),
                    'message' => $error->getMessage(),
                ],
                [...$violations],
            ));
        }

        return $object;
    }
}
