<?php

declare(strict_types=1);

namespace Keboola\SandboxesServiceApiClient\Tests\Sandboxes\Legacy;

use DateTimeImmutable;
use Generator;
use Keboola\SandboxesServiceApiClient\Sandboxes\Legacy\Sandbox;
use Keboola\SandboxesServiceApiClient\Sandboxes\Legacy\SandboxSizeParameters;
use PHPUnit\Framework\TestCase;

class SandboxTest extends TestCase
{
    public function testGetters(): void
    {
        $sandbox = Sandbox::fromArray([
            'id' => 'id',
            'componentId' => 'keboola.data-apps',
            'projectId' => 'project-id',
            'tokenId' => 'token-id',
            'type' => 'python',
            'active' => true,
            'shared' => true,
            'createdTimestamp' => '2024-02-02 12:00:00',
            'updatedTimestamp' => '2024-02-02 14:00:00',
            'deletedTimestamp' => '2024-02-02 16:00:00',
            'branchId' => 'branch-id',
            'configurationId' => 'configuration-id',
            'configurationVersion' => '4',
            'physicalId' => 'physical-id',
            'size' => 'small',
            'sizeParameters' => [
                'storageSize_GB' => 10,
            ],
            'imageVersion' => 'image-version',
            'packages' => ['foo', 'bar'],
            'expirationTimestamp' => '2024-02-02 18:00:00',
            'expirationAfterHours' => 1,
            'autoSuspendAfterSeconds' => 2,
            'user' => 'user',
            'password' => 'password',
            'host' => 'host',
            'url' => 'url',
            'stagingWorkspaceId' => 'staging-workspace-id',
            'stagingWorkspaceType' => 'staging-workspace-type',
            'workspaceDetails' => ['foo' => 'bar'],
            'lastAutosaveTimestamp' => '2024-02-02 20:00:00',
            'autosaveTokenId' => 'autosave-token-id',
            'databricks' => [
                'sparkVersion' => 'databricks-spark-version',
                'nodeType' => 'databricks-node-type',
                'numberOfNodes' => 5,
                'clusterId' => 'databricks-cluster-id',
            ],
            'persistentStorage' => [
                'pvcName' => 'pvc-name',
                'k8sManifest' => 'pvc-manifest',
            ],
        ]);

        self::assertSame('id', $sandbox->getId());
        self::assertSame('keboola.data-apps', $sandbox->getComponentId());
        self::assertSame('project-id', $sandbox->getProjectId());
        self::assertSame('small', $sandbox->getSize());
        self::assertTrue($sandbox->getActive());
        self::assertTrue($sandbox->getShared());
        self::assertSame('python', $sandbox->getType());
        self::assertSame('user', $sandbox->getUser());
        self::assertSame('password', $sandbox->getPassword());
        self::assertSame('host', $sandbox->getHost());
        self::assertSame('url', $sandbox->getUrl());
        self::assertSame('branch-id', $sandbox->getBranchId());
        self::assertSame('configuration-id', $sandbox->getConfigurationId());
        self::assertSame('4', $sandbox->getConfigurationVersion());
        self::assertSame('physical-id', $sandbox->getPhysicalId());
        self::assertSame('image-version', $sandbox->getImageVersion());
        self::assertSame(['foo', 'bar'], $sandbox->getPackages());
        self::assertSame('2024-02-02 12:00:00', $sandbox->getCreatedTimestamp());
        self::assertSame('2024-02-02 14:00:00', $sandbox->getUpdatedTimestamp());
        self::assertSame('2024-02-02 18:00:00', $sandbox->getExpirationTimestamp());
        self::assertSame(1, $sandbox->getExpirationAfterHours());
        self::assertSame(2, $sandbox->getAutoSuspendAfterSeconds());
        self::assertSame('2024-02-02 16:00:00', $sandbox->getDeletedTimestamp());
        self::assertSame('staging-workspace-id', $sandbox->getStagingWorkspaceId());
        self::assertSame('staging-workspace-type', $sandbox->getStagingWorkspaceType());
        self::assertSame(['foo' => 'bar'], $sandbox->getWorkspaceDetails());
        self::assertSame('2024-02-02 20:00:00', $sandbox->getLastAutosaveTimestamp());
        self::assertSame('autosave-token-id', $sandbox->getAutosaveTokenId());
        self::assertEquals((new SandboxSizeParameters())->setStorageSizeGB(10), $sandbox->getSizeParameters());
        self::assertSame('databricks-spark-version', $sandbox->getDatabricksSparkVersion());
        self::assertSame('databricks-node-type', $sandbox->getDatabricksNodeType());
        self::assertSame(5, $sandbox->getDatabricksNumberOfNodes());
        self::assertSame('databricks-cluster-id', $sandbox->getDatabricksClusterId());
        self::assertSame('pvc-name', $sandbox->getPersistentStoragePvcName());
        self::assertSame('pvc-manifest', $sandbox->getPersistentStorageK8sManifest());
    }

    public function testPasswordNullable(): void
    {
        $sandbox = new Sandbox();
        $nullPassword = $sandbox->getPassword();
        self::assertNull($nullPassword);

        $sandbox = Sandbox::fromArray([
            'id' => 1,
            'componentId' => 'component-id',
            'projectId' => '123',
            'tokenId' => '3453',
            'type' => 'python',
            'active' => true,
            'configurationVersion' => '1',
            'createdTimestamp' => (new DateTimeImmutable())->format('c'),
        ]);
        $nullPassword = $sandbox->getPassword();
        self::assertEmpty($nullPassword);
    }

    public function usesProxyDataProvider(): Generator
    {
        yield 'without url' => [
            'url' => '',
            'expectedResult' => false,
        ];

        yield 'without proxy' => [
            'url' => 'https://sandbox.keboola.com/',
            'expectedResult' => false,
        ];

        yield 'with proxy' => [
            'url' => 'https://123.hub.connection.keboola.com',
            'expectedResult' => true,
        ];
    }

    /**
     * @dataProvider usesProxyDataProvider
     */
    public function testUsesProxy(string $url, bool $expectedResult): void
    {
        self::assertSame(
            $expectedResult,
            (new Sandbox())->setUrl($url)->usesProxy(),
        );
    }

    public function getJupyterApiUrlReturnsNullIfUrlOrPasswordIsMissingDataProvider(): Generator
    {
        yield 'URL is empty' => [
            'url' => '',
            'password' => 'password',
        ];
        yield 'Password is empty' => [
            'url' => 'https://sandbox.hub.com/lab',
            'password' => '',
        ];
        yield 'Password is null' => [
            'url' => 'https://sandbox.hub.com/lab',
            'password' => null,
        ];
    }

    /**
     * @dataProvider getJupyterApiUrlReturnsNullIfUrlOrPasswordIsMissingDataProvider
     */
    public function testGetJupyterApiUrlReturnsNullIfUrlOrPasswordIsMissing(
        ?string $url,
        ?string $password,
    ): void {
        $sandbox = new Sandbox();
        $sandbox->setType('python');

        if ($url !== null) {
            $sandbox->setUrl($url);
        }
        if ($password !== null) {
            $sandbox->setPassword($password);
        }

        self::assertNull($sandbox->getJupyterApiUrl());
    }

    public function getJupyterApiUrlReturnsUrlForJupyterTypeSandboxDataProvider(): Generator
    {
        yield 'julia' => ['julia'];
        yield 'python' => ['python'];
        yield 'python-databricks' => ['python-databricks'];
        yield 'python-snowpark' => ['python-snowpark'];
        yield 'r' => ['r'];
    }

    /**
     * @dataProvider getJupyterApiUrlReturnsUrlForJupyterTypeSandboxDataProvider
     */
    public function testGetJupyterApiUrlReturnsUrlForJupyterTypeSandbox(
        string $type,
    ): void {
        $sandbox = new Sandbox();
        $sandbox->setPassword('password');
        $sandbox->setType($type);
        $sandbox->setUrl('https://sandbox.hub.com/dummy');

        self::assertSame('https://sandbox.hub.com/dummy', $sandbox->getJupyterApiUrl());

        // Tests if '/lab` suffix will be removed from sandbox url
        $sandbox = new Sandbox();
        $sandbox->setPassword('password');
        $sandbox->setType($type);
        $sandbox->setUrl('https://sandbox.hub.com/lab');

        self::assertSame('https://sandbox.hub.com', $sandbox->getJupyterApiUrl());
    }

    public function getJupyterApiUrlReturnsNullForNonJupyterTypeSandboxDataProvider(): Generator
    {
        yield 'bigquery' => ['bigquery'];
        yield 'exasol' => ['exasol'];
        yield 'redshift' => ['redshift'];
        yield 'snowflake' => ['snowflake'];
        yield 'synapse' => ['synapse'];
        yield 'teradata' => ['teradata'];
        yield 'test' => ['test'];
        yield 'streamlit' => ['streamlit'];
    }

    /**
     * @dataProvider getJupyterApiUrlReturnsNullForNonJupyterTypeSandboxDataProvider
     */
    public function testGetJupyterApiUrlReturnsNullForNonJupyterTypeSandbox(
        string $type,
    ): void {
        $sandbox = new Sandbox();
        $sandbox->setUrl('https://sandbox.hub.com/lab');
        $sandbox->setPassword('password');
        $sandbox->setType($type);

        self::assertNull($sandbox->getJupyterApiUrl());
    }
}
