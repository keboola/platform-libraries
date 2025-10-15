<?php

declare(strict_types=1);

namespace Keboola\PhpTestUtils\Fixtures\Dynamic;

use Keboola\PhpTestUtils\Fixtures\FixtureTraits\StorageApiAwareTrait;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ConfigurationRow;
use Symfony\Component\Uid\Uuid;

class VariableFixture implements FixtureInterface
{
    use StorageApiAwareTrait;
    private string $configurationId;
    private string $configurationRowId;

    public function initialize(): void
    {
        $componentsApi = new Components($this->getStorageClientWrapper()->getClientForDefaultBranch());

        $config = (new Configuration)
            ->setComponentId('keboola.variables')
            ->setName('Variable fixture ' . Uuid::v4())
            ->setConfiguration([
                'variables' => [
                    [
                        'name' => 'variable-1',
                        'type' => 'string',
                    ],
                    [
                        'name' => 'variable-2',
                        'type' => 'string',
                    ],
                ],
            ]);

        /** @var array{id: string} $configuration */
        $configuration = $componentsApi->addConfiguration($config);
        $config->setConfigurationId($configuration['id']);

        $configRow = new ConfigurationRow($config)
            ->setName('Variable fixture ' . Uuid::v4())
            ->setConfiguration([
                'values' => [
                    [
                        'name' => 'variable-1',
                        'value' => 'Value1',
                    ],
                    [
                        'name' => 'variable-2',
                        'value' => 'Value2',
                    ],
                ],
            ]);
        /** @var array{id: string} $configurationRow */
        $configurationRow = $componentsApi->addConfigurationRow($configRow);

        $this->configurationId = $configuration['id'];
        $this->configurationRowId = $configurationRow['id'];
    }

    public function cleanUp(): void
    {
        $componentsApi = new Components($this->getStorageClientWrapper()->getClientForDefaultBranch());
        $componentsApi->deleteConfiguration('keboola.variables', $this->configurationId);
    }

    public function getConfigId(): string
    {
        return $this->configurationId;
    }

    public function getConfigRowId(): string
    {
        return $this->configurationRowId;
    }
}
