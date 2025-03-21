<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\Util;

use Keboola\ApiBundle\Tests\Util\DummyClassicController;
use Keboola\ApiBundle\Tests\Util\DummyInvokeController;
use Keboola\ApiBundle\Util\ControllerReflector;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use Symfony\Component\DependencyInjection\Container;

class ControllerReflectorTest extends TestCase
{
    /**
     * @param null|array{string, string} $expectedMethod
     */
    #[DataProvider('provideControllerData')]
    public function testControllerMethodResolving(mixed $controller, mixed $expectedMethod): void
    {
        $container = new Container();
        $container->set('app.invoke_controller', new DummyInvokeController());
        $container->set('app.classic_controller', new DummyClassicController());

        $controllerReflector = new ControllerReflector($container);
        $result = $controllerReflector->resolveControllerMethod($controller);

        self::assertEquals($expectedMethod, $result);
    }

    public static function provideControllerData(): iterable
    {
        yield 'app.invoke_controller' => [
            'controller' => 'app.invoke_controller',
            'expectedMethod' => new ReflectionMethod(DummyInvokeController::class, '__invoke'),
        ];

        yield '[app.invoke_controller]' => [
            'controller' => ['app.invoke_controller'],
            'expectedMethod' => new ReflectionMethod(DummyInvokeController::class, '__invoke'),
        ];

        yield 'app.invoke_controller::__invoke' => [
            'controller' => 'app.invoke_controller::__invoke',
            'expectedMethod' => new ReflectionMethod(DummyInvokeController::class, '__invoke'),
        ];

        yield '[app.invoke_controller, __invoke]' => [
            'controller' => ['app.invoke_controller', '__invoke'],
            'expectedMethod' => new ReflectionMethod(DummyInvokeController::class, '__invoke'),
        ];

        yield 'app.classic_controller::handleRequest' => [
            'controller' => 'app.classic_controller::handleRequest',
            'expectedMethod' => new ReflectionMethod(DummyClassicController::class, 'handleRequest'),
        ];

        yield '[app.classic_controller, handleRequest]' => [
            'controller' => ['app.classic_controller', 'handleRequest'],
            'expectedMethod' => new ReflectionMethod(DummyClassicController::class, 'handleRequest'),
        ];

//        $functionController = function () {
//        };
//        yield 'function () {}' => [
//            'controller' => $functionController,
//            'result' => new ReflectionFunction($functionController),
//        ];

        yield 'new DummyInvokeController()' => [
            'controller' => new DummyInvokeController(),
            'expectedMethod' => new ReflectionMethod(DummyInvokeController::class, '__invoke'),
        ];

        yield '[new DummyInvokeController()]' => [
            'controller' => [new DummyInvokeController()],
            'expectedMethod' => new ReflectionMethod(DummyInvokeController::class, '__invoke'),
        ];

        yield '[new DummyClassicController(), handleRequest]' => [
            'controller' => [new DummyClassicController(), 'handleRequest'],
            'expectedMethod' => new ReflectionMethod(DummyClassicController::class, 'handleRequest'),
        ];
    }
}
