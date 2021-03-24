<?php

namespace Keboola\WorkspaceProvider;

use Keboola\InputMapping\Staging\Scope;
use Keboola\InputMapping\Staging\StrategyFactory as InputStrategyFactory;
use Keboola\OutputMapping\Staging\StrategyFactory as OutputStrategyFactory;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\WorkspaceProvider\Provider\ABSWorkspaceProvider;
use Keboola\WorkspaceProvider\Provider\LocalProvider;
use Keboola\WorkspaceProvider\Provider\RedshiftWorkspaceProvider;
use Keboola\WorkspaceProvider\Provider\SnowflakeWorkspaceProvider;
use Keboola\WorkspaceProvider\Provider\SynapseWorkspaceProvider;

class ProviderInitializer
{
    /** @var RedshiftWorkspaceProvider */
    private $redshiftWorkspaceProvider;

    /** @var SnowflakeWorkspaceProvider */
    private $snowflakeWorkspaceProvider;

    /** @var SynapseWorkspaceProvider */
    private $synapseWorkspaceProvider;

    /** @var ABSWorkspaceProvider */
    private $absWorkspaceProvider;

    /**
     * @param InputStrategyFactory $stagingFactory
     * @param string $stagingStorageInput
     * @param string $componentId
     * @param string $configId
     * @param array $tokenInfo
     * @param string $dataDirectory
     */
    public function initializeInputProviders(
        InputStrategyFactory $stagingFactory,
        $stagingStorageInput,
        $componentId,
        $configId,
        $tokenInfo,
        $dataDirectory
    ) {
        if (($stagingStorageInput === InputStrategyFactory::WORKSPACE_REDSHIFT) && $tokenInfo['owner']['hasRedshift']) {
            $stagingFactory->addProvider(
                $this->getRedshiftWorkspaceProvider($stagingFactory->getClientWrapper(), $componentId, $configId),
                [
                    InputStrategyFactory::WORKSPACE_REDSHIFT => new Scope([Scope::TABLE_DATA]),
                ]
            );
        }
        if (($stagingStorageInput === InputStrategyFactory::WORKSPACE_SNOWFLAKE) && $tokenInfo['owner']['hasSnowflake']) {
            $stagingFactory->addProvider(
                $this->getSnowflakeWorkspaceProvider($stagingFactory->getClientWrapper(), $componentId, $configId),
                [
                    InputStrategyFactory::WORKSPACE_SNOWFLAKE => new Scope([Scope::TABLE_DATA]),
                ]
            );
        }
        if (($stagingStorageInput === InputStrategyFactory::WORKSPACE_SYNAPSE) && $tokenInfo['owner']['hasSynapse']) {
            $stagingFactory->addProvider(
                $this->getSynapseWorkspaceProvider($stagingFactory->getClientWrapper(), $componentId, $configId),
                [
                    InputStrategyFactory::WORKSPACE_SYNAPSE => new Scope([Scope::TABLE_DATA]),
                ]
            );
        }
        if (($stagingStorageInput === InputStrategyFactory::WORKSPACE_ABS) &&
            ($tokenInfo['owner']['fileStorageProvider'] === 'azure')
        ) {
            $stagingFactory->addProvider(
                $this->getAbsWorkspaceProvider($stagingFactory->getClientWrapper(), $componentId, $configId),
                [
                    InputStrategyFactory::WORKSPACE_ABS => new Scope([Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_DATA]),
                ]
            );
        }
        $stagingFactory->addProvider(
            new LocalProvider($dataDirectory),
            [
                InputStrategyFactory::LOCAL => new Scope([Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_DATA, Scope::TABLE_METADATA]),
                // TABLE_DATA for ABS and S3 is bound to LocalProvider because it requires no provider at all
                InputStrategyFactory::S3 => new Scope([Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_DATA, Scope::TABLE_METADATA]),
                InputStrategyFactory::ABS => new Scope([Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_DATA, Scope::TABLE_METADATA]),
                InputStrategyFactory::WORKSPACE_REDSHIFT => new Scope([Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_METADATA]),
                InputStrategyFactory::WORKSPACE_SYNAPSE => new Scope([Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_METADATA]),
                InputStrategyFactory::WORKSPACE_SNOWFLAKE => new Scope([Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_METADATA]),
                InputStrategyFactory::WORKSPACE_ABS => new Scope([Scope::TABLE_METADATA]),
            ]
        );
    }

    /**
     * @param OutputStrategyFactory $stagingFactory
     * @param string $stagingStorageOutput
     * @param string $componentId
     * @param string $configId
     * @param array $tokenInfo
     * @param string $dataDirectory
     */
    public function initializeOutputProviders(
        OutputStrategyFactory $stagingFactory,
        $stagingStorageOutput,
        $componentId,
        $configId,
        $tokenInfo,
        $dataDirectory
    ) {
        if (($stagingStorageOutput === OutputStrategyFactory::WORKSPACE_REDSHIFT) && $tokenInfo['owner']['hasRedshift']) {
            $stagingFactory->addProvider(
                $this->getRedshiftWorkspaceProvider($stagingFactory->getClientWrapper(), $componentId, $configId),
                [
                    OutputStrategyFactory::WORKSPACE_REDSHIFT => new Scope([Scope::TABLE_DATA]),
                ]
            );
        }
        if (($stagingStorageOutput === OutputStrategyFactory::WORKSPACE_SNOWFLAKE) && $tokenInfo['owner']['hasSnowflake']) {
            $stagingFactory->addProvider(
                $this->getSnowflakeWorkspaceProvider($stagingFactory->getClientWrapper(), $componentId, $configId),
                [
                    OutputStrategyFactory::WORKSPACE_SNOWFLAKE => new Scope([Scope::TABLE_DATA]),
                ]
            );
        }
        if (($stagingStorageOutput === OutputStrategyFactory::WORKSPACE_SYNAPSE) && $tokenInfo['owner']['hasSynapse']) {
            $stagingFactory->addProvider(
                $this->getSynapseWorkspaceProvider($stagingFactory->getClientWrapper(), $componentId, $configId),
                [
                    OutputStrategyFactory::WORKSPACE_SYNAPSE => new Scope([Scope::TABLE_DATA]),
                ]
            );
        }
        if (($stagingStorageOutput === OutputStrategyFactory::WORKSPACE_ABS) &&
            ($tokenInfo['owner']['fileStorageProvider'] === 'azure')
        ) {
            $stagingFactory->addProvider(
                $this->getAbsWorkspaceProvider($stagingFactory->getClientWrapper(), $componentId, $configId),
                [
                    OutputStrategyFactory::WORKSPACE_ABS => new Scope([Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_DATA]),
                ]
            );
        }
        $stagingFactory->addProvider(
            new LocalProvider($dataDirectory),
            [
                OutputStrategyFactory::LOCAL => new Scope([Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_DATA, Scope::TABLE_METADATA]),
                OutputStrategyFactory::WORKSPACE_REDSHIFT => new Scope([Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_METADATA]),
                OutputStrategyFactory::WORKSPACE_SYNAPSE => new Scope([Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_METADATA]),
                OutputStrategyFactory::WORKSPACE_SNOWFLAKE => new Scope([Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_METADATA]),
                OutputStrategyFactory::WORKSPACE_ABS => new Scope([Scope::TABLE_METADATA]),
            ]
        );
    }

    /**
     * @param ClientWrapper $clientWrapper
     * @param string $componentId
     * @param string $configId
     * @return RedshiftWorkspaceProvider
     */
    private function getRedshiftWorkspaceProvider(ClientWrapper $clientWrapper, $componentId, $configId)
    {
        if (!$this->redshiftWorkspaceProvider) {
            $this->redshiftWorkspaceProvider = new RedshiftWorkspaceProvider($clientWrapper->getBasicClient(), $componentId, $configId);
        }
        return $this->redshiftWorkspaceProvider;
    }

    /**
     * @param ClientWrapper $clientWrapper
     * @param string $componentId
     * @param string $configId
     * @return SnowflakeWorkspaceProvider
     */
    private function getSnowflakeWorkspaceProvider(ClientWrapper $clientWrapper, $componentId, $configId)
    {
        if (!$this->snowflakeWorkspaceProvider) {
            $this->snowflakeWorkspaceProvider = new SnowflakeWorkspaceProvider($clientWrapper->getBasicClient(), $componentId, $configId);
        }
        return $this->snowflakeWorkspaceProvider;
    }

    /**
     * @param ClientWrapper $clientWrapper
     * @param string $componentId
     * @param string $configId
     * @return SynapseWorkspaceProvider
     */
    private function getSynapseWorkspaceProvider(ClientWrapper $clientWrapper, $componentId, $configId)
    {
        if (!$this->synapseWorkspaceProvider) {
            $this->synapseWorkspaceProvider = new SynapseWorkspaceProvider($clientWrapper->getBasicClient(), $componentId, $configId);
        }
        return $this->synapseWorkspaceProvider;
    }

    /**
     * @param ClientWrapper $clientWrapper
     * @param string $componentId
     * @param string $configId
     * @return ABSWorkspaceProvider
     */
    private function getAbsWorkspaceProvider(ClientWrapper $clientWrapper, $componentId, $configId)
    {
        if (!$this->absWorkspaceProvider) {
            $this->absWorkspaceProvider = new ABSWorkspaceProvider($clientWrapper->getBasicClient(), $componentId, $configId);
        }
        return $this->absWorkspaceProvider;
    }
}
