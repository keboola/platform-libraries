<?php

declare(strict_types=1);

namespace Keboola\SandboxesServiceApiClient\Tests\Apps;

use Generator;
use Keboola\ApiClientBase\Exception\ClientException;
use Keboola\SandboxesServiceApiClient\Apps\App;
use PHPUnit\Framework\TestCase;

class AppTest extends TestCase
{
    public function testGetters(): void
    {
        $app = App::fromResponseData([
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
    }

    public function testGettersWithNullableValues(): void
    {
        $app = App::fromResponseData([
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
            'type' => null,
            'branchId' => 'branch-id',
            'configId' => 'config-id',
            'configVersion' => '5',
            'state' => 'running',
            'desiredState' => 'running',
            'lastRequestTimestamp' => '2024-02-02T12:00:00+01:00',
            'url' => 'https://example.com',
            'autoSuspendAfterSeconds' => 3600,
        ];

        $app = App::fromResponseData($expectedData);

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
        ];

        unset($data[$missingProperty]);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage("Property $missingProperty is missing from API response");

        App::fromResponseData($data);
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

    public function testFromResponseDataWithEmptyAutoSuspendAfterSeconds(): void
    {
        // Test missing autoSuspendAfterSeconds defaults to 0
        $app = App::fromResponseData([
            'id' => 'app-id',
            'projectId' => 'project-id',
            'componentId' => 'keboola.data-apps',
            'configId' => 'config-id',
            'configVersion' => '5',
            'state' => 'running',
            'desiredState' => 'running',
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
    }

    public function testFromResponseDataCastsNumericValuesToStrings(): void
    {
        // Mutation testing: verify that numeric values from API response are cast to strings
        $app = App::fromResponseData([
            'id' => 12345,
            'projectId' => 67890,
            'componentId' => 99999,
            'configId' => 111,
            'configVersion' => 5,
            'state' => 'running',
            'desiredState' => 'running',
            'type' => 42,
            'autoSuspendAfterSeconds' => '3600',
        ]);

        self::assertSame('12345', $app->getId());
        self::assertSame('67890', $app->getProjectId());
        self::assertSame('99999', $app->getComponentId());
        self::assertSame('111', $app->getConfigId());
        self::assertSame('5', $app->getConfigVersion());
        self::assertSame('42', $app->getType());
        self::assertSame(3600, $app->getAutoSuspendAfterSeconds());
    }

    public function testSetTypeIsCallablePublicly(): void
    {
        // PublicVisibility mutation: setType must be accessible from outside the class
        $app = new App();
        $result = $app->setType('streamlit');

        self::assertSame('streamlit', $app->getType());
        self::assertSame($app, $result);
    }

    public function testSetTypeAcceptsNull(): void
    {
        $app = new App();
        $app->setType('streamlit');
        $result = $app->setType(null);

        self::assertNull($app->getType());
        self::assertSame($app, $result);
    }
}
