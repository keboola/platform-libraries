<?php

declare(strict_types=1);

namespace Keboola\PhpTestUtils\Fixtures\Dynamic;

use Keboola\PhpTestUtils\Fixtures\FixtureTraits\EntityManagerTrait;
use Keboola\PhpTestUtils\Fixtures\FixtureTraits\StorageApiAwareTrait;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ConfigurationRow;
use Symfony\Component\Uid\Uuid;

class SharedCodeFixture implements FixtureInterface
{
    use StorageApiAwareTrait;
    use EntityManagerTrait;
    private string $configrationId;
    private string $configurationRowId;

    public function initialize(): void
    {
        $componentsApi = new Components($this->getStorageClientWrapper()->getClientForDefaultBranch());

        $config = (new Configuration)
            ->setComponentId('keboola.shared-code')
            ->setName('Shared Code fixture ' . Uuid::v4())
            ->setConfiguration([
                'componentId' => 'keboola.snowflake-transformation',
            ]);

        /** @var array{id: string} $configuration */
        $configuration = $componentsApi->addConfiguration($config);
        $config->setConfigurationId($configuration['id']);

        $configRow = new ConfigurationRow($config)
            ->setName('Shared Code fixture ' . Uuid::v4())
            ->setConfiguration([
                'code_content' => 'SELECT 1;',
            ]);
        /** @var array{id: string} $configurationRow */
        $configurationRow = $componentsApi->addConfigurationRow($configRow);

        $this->configrationId = $configuration['id'];
        $this->configurationRowId = $configurationRow['id'];
    }

    public function cleanUp(): void
    {
        $componentsApi = new Components($this->getStorageClientWrapper()->getClientForDefaultBranch());
        $componentsApi->deleteConfiguration('keboola.shared-code', $this->configrationId);
    }

    public function getConfigId(): string
    {
        return $this->configrationId;
    }

    public function getConfigRowId(): string
    {
        return $this->configurationRowId;
    }
}
