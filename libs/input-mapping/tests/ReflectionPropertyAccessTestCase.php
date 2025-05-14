<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests;

use ReflectionClass;
use ReflectionException;
use ReflectionProperty;

trait ReflectionPropertyAccessTestCase
{
    protected static function setPrivatePropertyValue(object $object, string $property, mixed $value): void
    {
        $reflection = self::findPropertyOnClass(new ReflectionClass($object), $property);
        $reflection->setAccessible(true);
        $reflection->setValue($object, $value);
        $reflection->setAccessible(false);
    }

    protected static function getPrivatePropertyValue(object $object, string $property): mixed
    {
        $reflection = self::findPropertyOnClass(new ReflectionClass($object), $property);
        $reflection->setAccessible(true);
        $value = $reflection->getValue($object);
        $reflection->setAccessible(false);
        return $value;
    }

    /**
     * @param class-string $class
     */
    protected static function setPrivateStaticPropertyValue(string $class, string $property, mixed $value): void
    {
        $reflection = self::findPropertyOnClass(new ReflectionClass($class), $property);
        $reflection->setAccessible(true);
        $reflection->setValue($value);
        $reflection->setAccessible(false);
    }

    /**
     * @param class-string $class
     */
    protected static function getPrivateStaticPropertyValue(string $class, string $property): mixed
    {
        $reflection = self::findPropertyOnClass(new ReflectionClass($class), $property);
        $reflection->setAccessible(true);
        $value = $reflection->getValue();
        $reflection->setAccessible(false);
        return $value;
    }

    /**
     * @param ReflectionClass<object> $class
     */
    private static function findPropertyOnClass(ReflectionClass $class, string $propertyName): ReflectionProperty
    {
        try {
            return $class->getProperty($propertyName);
        } catch (ReflectionException $e) {
            $parent = $class->getParentClass();
            if ($parent !== false) {
                return self::findPropertyOnClass($parent, $propertyName);
            }

            throw $e;
        }
    }
}
