<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Util;

use ReflectionException;
use ReflectionFunction;
use ReflectionMethod;
use Symfony\Component\DependencyInjection\ContainerInterface;

class ControllerReflector
{
    public function __construct(
        private readonly ContainerInterface $container
    ) {
    }

    /**
     * @param mixed $controllerSpec
     * @return ReflectionMethod|null
     */
    public function resolveControllerMethod($controllerSpec): ?ReflectionMethod
    {
        if (is_string($controllerSpec)) {
            $controllerSpec = explode('::', $controllerSpec);
        }

        if (is_array($controllerSpec)) {
            switch (count($controllerSpec)) {
                case 1:
                    [$controllerClass] = $controllerSpec;
                    $controllerMethod = '__invoke';
                    break;

                case 2:
                    [$controllerClass, $controllerMethod] = $controllerSpec;
                    break;

                default:
                    return null;
            }

            if (is_string($controllerClass)) {
                if (!$this->container->has($controllerClass)) {
                    return null;
                }

                $controllerClass = $this->container->get($controllerClass);
            }

            try {
                return new ReflectionMethod($controllerClass, $controllerMethod);
            } catch (ReflectionException) {
                // In case we can't reflect the controller, we just ignore the route
                return null;
            }
        }

        if (is_object($controllerSpec) && method_exists($controllerSpec, '__invoke')) {
            try {
                return new ReflectionMethod($controllerSpec, '__invoke');
            } catch (ReflectionException) {
                // In case we can't reflect the controller, we just ignore the route
                return null;
            }
        }

        return null;
    }
}
