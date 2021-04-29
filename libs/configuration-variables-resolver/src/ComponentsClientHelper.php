<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver;

use Keboola\ConfigurationVariablesResolver\Configuration\Variables;
use Keboola\ConfigurationVariablesResolver\Configuration\VariableValues;
use Keboola\ConfigurationVariablesResolver\Exception\UserException;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Components;
use Keboola\StorageApiBranch\ClientWrapper;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class ComponentsClientHelper
{
    const KEBOOLA_VARIABLES = 'keboola.variables';

    private ClientWrapper $clientWrapper;

    public function __construct(ClientWrapper $clientWrapper)
    {
        $this->clientWrapper = $clientWrapper;
    }

    private function getClient(): Components
    {
        if ($this->clientWrapper->hasBranch()) {
            return new Components($this->clientWrapper->getBranchClient());
        }
        return new Components($this->clientWrapper->getBasicClient());
    }

    public function getVariablesConfiguration(string $variablesId): array
    {
        try {
            $vConfiguration = $this->getClient()->getConfiguration(self::KEBOOLA_VARIABLES, $variablesId);
            return (new Variables())->processData([
                'config' => $vConfiguration['configuration'],
            ]);
        } catch (ClientException $e) {
            throw new UserException('Variable configuration cannot be read: ' . $e->getMessage(), $e);
        } catch (InvalidConfigurationException $e) {
            throw new UserException('Variable configuration is invalid: ' . $e->getMessage(), $e);
        }
    }

    public function getVariablesConfigurationRow(string $configurationId, string $rowId): array
    {
        try {
            $vRow = $this->getClient()->getConfigurationRow(
                self::KEBOOLA_VARIABLES,
                $configurationId,
                $rowId
            );
            return (new VariableValues())->processData(['config' => $vRow]);
        } catch (ClientException $e) {
            throw new UserException(
                sprintf(
                    'Cannot read requested variable values "%s" for row "%s".',
                    $configurationId,
                    $rowId
                ),
                $e
            );
        } catch (InvalidConfigurationException $e) {
            throw new UserException('Variable values configuration is invalid: ' . $e->getMessage(), $e);
        }
    }
}
