<?php

declare(strict_types=1);

namespace Keboola\SandboxesServiceApiClient\Tests\Apps;

use Generator;
use Keboola\SandboxesServiceApiClient\Apps\App;
use Keboola\SandboxesServiceApiClient\Exception\ClientException;
use PHPUnit\Framework\TestCase;

class AppTest extends TestCase
{
    public function testGetters(): void
    {
        $app = App::fromArray([
            'id' => 'app-id',
            'projectId' => 'project-id',
            'componentId' => 'keboola.data-apps',
            'branchId' => 'branch-id',
            'configId' => 'config-id',
            'configVersion' => '5',
            'state' => 'running',
            'desiredState' => 'running',
            'lastRequestTimestamp' => '2024-02-02T12:00:00+01:00',
            'url' => 'https://example.com',
            'autoSuspendAfterSeconds' => 3600,
            'provisioningStrategy' => 'operator',
        ]);

        self::assertSame('app-id', $app->getId());
        self::assertSame('project-id', $app->getProjectId());
        self::assertSame('keboola.data-apps', $app->getComponentId());
        self::assertSame('branch-id', $app->getBranchId());
        self::assertSame('config-id', $app->getConfigId());
        self::assertSame('5', $app->getConfigVersion());
        self::assertSame('running', $app->getState());
        self::assertSame('running', $app->getDesiredState());
        self::assertSame('2024-02-02T12:00:00+01:00', $app->getLastRequestTimestamp());
        self::assertSame('https://example.com', $app->getUrl());
        self::assertSame(3600, $app->getAutoSuspendAfterSeconds());
        self::assertSame('operator', $app->getProvisioningStrategy());
    }

    public function testGettersWithNullableValues(): void
    {
        $app = App::fromArray([
            'id' => 'app-id',
            'projectId' => 'project-id',
            'componentId' => 'keboola.data-apps',
            'branchId' => null,
            'configId' => 'config-id',
            'configVersion' => '5',
            'state' => 'stopped',
            'desiredState' => 'stopped',
            'lastRequestTimestamp' => null,
            'url' => null,
            'autoSuspendAfterSeconds' => 0,
            'provisioningStrategy' => 'jobQueue',
        ]);

        self::assertNull($app->getBranchId());
        self::assertNull($app->getLastRequestTimestamp());
        self::assertNull($app->getUrl());
        self::assertSame(0, $app->getAutoSuspendAfterSeconds());
    }

    public function testToArray(): void
    {
        $expectedData = [
            'id' => 'app-id',
            'projectId' => 'project-id',
            'componentId' => 'keboola.data-apps',
            'branchId' => 'branch-id',
            'configId' => 'config-id',
            'configVersion' => '5',
            'state' => 'running',
            'desiredState' => 'running',
            'lastRequestTimestamp' => '2024-02-02T12:00:00+01:00',
            'url' => 'https://example.com',
            'autoSuspendAfterSeconds' => 3600,
            'provisioningStrategy' => 'operator',
        ];

        $app = App::fromArray($expectedData);

        self::assertSame($expectedData, $app->toArray());
    }

    /**
     * @return Generator<string, array{string}>
     */
    public function requiredPropertiesDataProvider(): Generator
    {
        $requiredProps = [
            'id',
            'projectId',
            'componentId',
            'configId',
            'configVersion',
            'state',
            'desiredState',
            'provisioningStrategy',
        ];

        foreach ($requiredProps as $property) {
            yield "missing $property" => [$property];
        }
    }

    /**
     * @dataProvider requiredPropertiesDataProvider
     */
    public function testMissingRequiredProperties(string $missingProperty): void
    {
        $data = [
            'id' => 'app-id',
            'projectId' => 'project-id',
            'componentId' => 'keboola.data-apps',
            'configId' => 'config-id',
            'configVersion' => '5',
            'state' => 'running',
            'desiredState' => 'running',
            'provisioningStrategy' => 'operator',
        ];

        unset($data[$missingProperty]);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage("Property $missingProperty is missing from API response");

        App::fromArray($data);
    }

    /**
     * @return Generator<string, array{string}>
     */
    public function invalidStatesDataProvider(): Generator
    {
        yield 'invalid state' => ['invalid'];
        yield 'empty state' => [''];
        yield 'random state' => ['random'];
    }

    /**
     * @dataProvider invalidStatesDataProvider
     */
    public function testInvalidState(string $invalidState): void
    {
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Invalid state');

        $app = new App();
        $app->setState($invalidState);
    }

    /**
     * @return Generator<string, array{string}>
     */
    public function validStatesDataProvider(): Generator
    {
        foreach (App::VALID_STATES as $state) {
            yield $state => [$state];
        }
    }

    /**
     * @dataProvider validStatesDataProvider
     */
    public function testValidState(string $validState): void
    {
        $app = new App();
        $result = $app->setState($validState);

        self::assertSame($validState, $app->getState());
        self::assertSame($app, $result);
    }

    /**
     * @return Generator<string, array{string}>
     */
    public function invalidDesiredStatesDataProvider(): Generator
    {
        yield 'invalid desired state' => ['invalid'];
        yield 'empty desired state' => [''];
        yield 'created state' => ['created'];
    }

    /**
     * @dataProvider invalidDesiredStatesDataProvider
     */
    public function testInvalidDesiredState(string $invalidDesiredState): void
    {
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Invalid desired state');

        $app = new App();
        $app->setDesiredState($invalidDesiredState);
    }

    /**
     * @return Generator<string, array{string}>
     */
    public function validDesiredStatesDataProvider(): Generator
    {
        foreach (App::VALID_DESIRED_STATES as $desiredState) {
            yield $desiredState => [$desiredState];
        }
    }

    /**
     * @dataProvider validDesiredStatesDataProvider
     */
    public function testValidDesiredState(string $validDesiredState): void
    {
        $app = new App();
        $result = $app->setDesiredState($validDesiredState);

        self::assertSame($validDesiredState, $app->getDesiredState());
        self::assertSame($app, $result);
    }

    /**
     * @return Generator<string, array{string}>
     */
    public function invalidProvisioningStrategiesDataProvider(): Generator
    {
        yield 'invalid strategy' => ['invalid'];
        yield 'empty strategy' => [''];
        yield 'kubernetes strategy' => ['kubernetes'];
    }

    /**
     * @dataProvider invalidProvisioningStrategiesDataProvider
     */
    public function testInvalidProvisioningStrategy(string $invalidStrategy): void
    {
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Invalid provisioning strategy');

        $app = new App();
        $app->setProvisioningStrategy($invalidStrategy);
    }

    /**
     * @return Generator<string, array{string}>
     */
    public function validProvisioningStrategiesDataProvider(): Generator
    {
        foreach (App::VALID_PROVISIONING_STRATEGIES as $strategy) {
            yield $strategy => [$strategy];
        }
    }

    /**
     * @dataProvider validProvisioningStrategiesDataProvider
     */
    public function testValidProvisioningStrategy(string $validStrategy): void
    {
        $app = new App();
        $result = $app->setProvisioningStrategy($validStrategy);

        self::assertSame($validStrategy, $app->getProvisioningStrategy());
        self::assertSame($app, $result);
    }

    public function testSettersReturnSelfForChaining(): void
    {
        $app = new App();

        self::assertSame($app, $app->setId('test'));
        self::assertSame($app, $app->setProjectId('test'));
        self::assertSame($app, $app->setComponentId('test'));
        self::assertSame($app, $app->setBranchId('test'));
        self::assertSame($app, $app->setConfigId('test'));
        self::assertSame($app, $app->setConfigVersion('test'));
        self::assertSame($app, $app->setLastRequestTimestamp('test'));
        self::assertSame($app, $app->setUrl('test'));
        self::assertSame($app, $app->setAutoSuspendAfterSeconds(123));
    }

    public function testFromArrayWithEmptyAutoSuspendAfterSeconds(): void
    {
        // Test missing autoSuspendAfterSeconds defaults to 0
        $app = App::fromArray([
            'id' => 'app-id',
            'projectId' => 'project-id',
            'componentId' => 'keboola.data-apps',
            'configId' => 'config-id',
            'configVersion' => '5',
            'state' => 'running',
            'desiredState' => 'running',
            'provisioningStrategy' => 'operator',
        ]);

        self::assertSame(0, $app->getAutoSuspendAfterSeconds());
    }

    public function testValidationErrorContainsAllValidValues(): void
    {
        $app = new App();

        try {
            $app->setState('invalid');
            self::fail('Expected exception was not thrown');
        } catch (ClientException $e) {
            $message = $e->getMessage();
            foreach (App::VALID_STATES as $state) {
                self::assertStringContainsString($state, $message);
            }
        }

        try {
            $app->setDesiredState('invalid');
            self::fail('Expected exception was not thrown');
        } catch (ClientException $e) {
            $message = $e->getMessage();
            foreach (App::VALID_DESIRED_STATES as $state) {
                self::assertStringContainsString($state, $message);
            }
        }

        try {
            $app->setProvisioningStrategy('invalid');
            self::fail('Expected exception was not thrown');
        } catch (ClientException $e) {
            $message = $e->getMessage();
            foreach (App::VALID_PROVISIONING_STRATEGIES as $strategy) {
                self::assertStringContainsString($strategy, $message);
            }
        }
    }
}
