<?php

declare(strict_types=1);

namespace Keboola\PhpTestUtils\Fixtures\Dynamic;

use Keboola\PhpTestUtils\Fixtures\FixtureTraits\StorageApiAwareTrait;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ConfigurationRow;

class ConfigurationWithRowFixture implements FixtureInterface
{
    use StorageApiAwareTrait;

    private const string TEST_COMPONENT_ID = 'keboola.runner-config-test';
    private string $configurationId;
    private string $componentId;
    private string $defaultBranchId;
    private string $rowId;

    public function initialize(): void
    {
        $componentsApi = new Components($this->getStorageClientWrapper()->getClientForDefaultBranch());
        $config = (new Configuration)
            ->setComponentId(self::TEST_COMPONENT_ID)
             ->setName(__CLASS__)
           ->setConfiguration([
                'parameters' => [
                    'operation' => 'sleep',
                    'timeout' => 5,
                ],
            ]);
        $configuration = $componentsApi->addConfiguration($config);
        assert(is_array($configuration));
        assert(is_scalar($configuration['id']));
        $config->setConfigurationId((string) $configuration['id']);

        $configRow = (new ConfigurationRow($config))
            ->setName('Row 1')
            ->setDescription('this row does contains important information')
            ->setConfiguration([
                'parameters' => [
                    'operation' => 'sleep-row',
                ],
            ]);
        $row = $componentsApi->addConfigurationRow($configRow);
        assert(is_array($row));
        assert(is_scalar($row['id']));
        $rowId = (string) $row['id'];

        $this->configurationId = (string) $configuration['id'];
        $this->componentId = self::TEST_COMPONENT_ID;
        $this->rowId = $rowId;
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

    public function getRowId(): string
    {
        return $this->rowId;
    }
}
