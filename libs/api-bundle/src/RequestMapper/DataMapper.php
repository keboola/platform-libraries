<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\RequestMapper;

use CuyZ\Valinor\Mapper\MappingError;
use CuyZ\Valinor\Mapper\Tree\Message\Messages;
use CuyZ\Valinor\Mapper\Tree\Message\NodeMessage;
use CuyZ\Valinor\MapperBuilder;
use Keboola\ApiBundle\RequestMapper\Attribute\RequestKey;
use Keboola\ApiBundle\RequestMapper\Exception\InvalidPayloadException;
use Keboola\ApiBundle\RequestMapper\Exception\RequestMapperException;
use ReflectionClass;
use ReflectionNamedType;
use Symfony\Component\DependencyInjection\Attribute\Autowire;
use Symfony\Component\Validator\ConstraintViolationInterface;
use Symfony\Component\Validator\Validator\ValidatorInterface;

class DataMapper
{
    /** @var array<class-string, array<non-empty-string, non-empty-string>> */
    private array $keyMapCache = [];

    public function __construct(
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
        $keyMap = $this->keyMap($objectType);
        if ($keyMap !== []) {
            $data = self::applyKeyMap($data, $keyMap);
        }
        $sourceKeys = array_flip($keyMap);

        $mapperBuilder = $this->mapperBuilder;
        if ($enableFlexibleCasting) {
            $mapperBuilder = $mapperBuilder->enableFlexibleCasting();
        }
        if ($enableExtraKeys) {
            $mapperBuilder = $mapperBuilder->allowSuperfluousKeys();
        }
        $mapperBuilder = $mapperBuilder->allowPermissiveTypes();

        try {
            $object = $mapperBuilder->mapper()->map($objectType, $data);
        } catch (MappingError $e) {
            throw new InvalidPayloadException(
                $errorMessage,
                $errorCode,
                array_map(
                    fn(NodeMessage $message) => [
                        'path' => self::toSourcePath($message->node()->path(), $sourceKeys),
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
                    'path' => self::toSourcePath($error->getPropertyPath(), $sourceKeys),
                    'message' => $error->getMessage(),
                ],
                [...$violations],
            ));
        }

        return $object;
    }

    /**
     * Wire key => property name, collected from {@see RequestKey} attributes on the mapped class,
     * memoized per class.
     *
     * Promoted constructor properties expose the attribute on both the property and the parameter;
     * plain properties only on the property. Scanning properties therefore covers both.
     *
     * @param class-string $objectType
     * @return array<non-empty-string, non-empty-string>
     */
    private function keyMap(string $objectType): array
    {
        if (array_key_exists($objectType, $this->keyMapCache)) {
            return $this->keyMapCache[$objectType];
        }

        ['keys' => $keys, 'nested' => $nested] = self::inspectClass($objectType);
        self::assertNoNestedRequestKeys($objectType, $nested);

        return $this->keyMapCache[$objectType] = $keys;
    }

    /**
     * Single reflection pass over a class, collecting both the declared request keys and the
     * property types that need walking for the nested-declaration check.
     *
     * @param class-string $objectType
     * @return array{keys: array<non-empty-string, non-empty-string>, nested: list<class-string>}
     */
    private static function inspectClass(string $objectType): array
    {
        $keys = [];
        $nested = [];

        foreach ((new ReflectionClass($objectType))->getProperties() as $property) {
            $attributes = $property->getAttributes(RequestKey::class);
            if ($attributes !== []) {
                $key = $attributes[0]->newInstance()->key;
                if (isset($keys[$key])) {
                    throw new RequestMapperException(sprintf(
                        'Class "%s" maps request key "%s" to both "%s" and "%s"',
                        $objectType,
                        $key,
                        $keys[$key],
                        $property->getName(),
                    ));
                }

                $keys[$key] = $property->getName();
            }

            $type = $property->getType();

            // Unions and intersections carry no single resolvable name; builtins have no properties.
            if (!$type instanceof ReflectionNamedType || $type->isBuiltin()) {
                continue;
            }

            $name = $type->getName();
            if (class_exists($name)) {
                $nested[] = $name;
            }
        }

        return ['keys' => $keys, 'nested' => $nested];
    }

    /**
     * The key map is applied to the top-level payload only. A {@see RequestKey} declared deeper
     * would be silently ignored — and worse, would invert its own guarantee, because the bare
     * property name would then be accepted while the declared wire key was not. Reject it instead.
     *
     * Only statically resolvable property types are walked; element types of collections and union
     * members cannot be resolved without Valinor's type parser, so a nested declaration reachable
     * only through those is not detected.
     *
     * @param class-string $objectType
     * @param list<class-string> $queue Class-typed properties of the root, from {@see inspectClass}.
     */
    private static function assertNoNestedRequestKeys(string $objectType, array $queue): void
    {
        $visited = [$objectType => true];

        while ($queue !== []) {
            $nestedType = array_shift($queue);
            if (isset($visited[$nestedType])) {
                continue;
            }
            $visited[$nestedType] = true;

            ['keys' => $keys, 'nested' => $nested] = self::inspectClass($nestedType);
            if ($keys !== []) {
                throw new RequestMapperException(sprintf(
                    'Class "%s" declares #[RequestKey] on nested payload object "%s"; ' .
                    'only top-level payload properties are supported',
                    $objectType,
                    $nestedType,
                ));
            }

            foreach ($nested as $furtherType) {
                $queue[] = $furtherType;
            }
        }
    }

    /**
     * Renames source keys to the property names the mapper expects.
     *
     * A value sent directly under a rename *target* is dropped: only the declared source key may
     * populate the property, so the outcome never depends on the order of keys in the payload.
     *
     * @param array<non-empty-string, non-empty-string> $keyMap
     */
    private static function applyKeyMap(mixed $data, array $keyMap): mixed
    {
        if (!is_iterable($data)) {
            return $data;
        }

        /** @var iterable<array-key, mixed> $data */
        $result = [];
        foreach ($data as $key => $value) {
            if (in_array($key, $keyMap, true)) {
                continue;
            }

            $result[$keyMap[$key] ?? $key] = $value;
        }

        return $result;
    }

    /**
     * Reports an error against the key the client actually sent rather than the property name.
     *
     * @param array<non-empty-string, non-empty-string> $sourceKeys Property name => source key.
     */
    private static function toSourcePath(string $path, array $sourceKeys): string
    {
        if ($sourceKeys === []) {
            return $path;
        }

        // Only the first segment can be renamed; the map applies to top-level payload keys.
        $segments = explode('.', $path, 2);
        $segments[0] = $sourceKeys[$segments[0]] ?? $segments[0];

        return implode('.', $segments);
    }
}
