<?php

declare(strict_types=1);

namespace Keboola\PhpTestUtils\Fixtures\Dynamic;

use Keboola\PhpTestUtils\Fixtures\FixtureTraits\StorageApiAwareTrait;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;

class ConfigurationFixture implements FixtureInterface
{
    use StorageApiAwareTrait;

    private const string TEST_COMPONENT_ID = 'keboola.runner-config-test';
    private string $configurationId;
    private string $componentId;
    private string $defaultBranchId;

    public function initialize(): void
    {
        $componentsApi = new Components($this->getStorageClientWrapper()->getClientForDefaultBranch());
        $config = (new Configuration)
            ->setComponentId(self::TEST_COMPONENT_ID)
            ->setName(__CLASS__)
            ->setConfiguration([
                'parameters' => [
                    'config' => [
                        'test' => 'value',
                    ],
                ],
            ]);
        $configuration = $componentsApi->addConfiguration($config);
        assert(is_array($configuration));
        assert(is_scalar($configuration['id']));
        $this->configurationId = (string) $configuration['id'];

        $this->componentId = self::TEST_COMPONENT_ID;
        $this->defaultBranchId = $this->getStorageClientWrapper()->getDefaultBranch()->id;
    }

    public function cleanUp(): void
    {
        $componentsApi = new Components($this->getStorageClientWrapper()->getClientForDefaultBranch());
        // delete
        $componentsApi->deleteConfiguration(self::TEST_COMPONENT_ID, $this->configurationId);
        // purge
        $componentsApi->deleteConfiguration(self::TEST_COMPONENT_ID, $this->configurationId);
    }

    public function getConfigurationId(): string
    {
        return $this->configurationId;
    }

    public function getComponentId(): string
    {
        return $this->componentId;
    }

    public function getDefaultBranchId(): string
    {
        return $this->defaultBranchId;
    }
}
