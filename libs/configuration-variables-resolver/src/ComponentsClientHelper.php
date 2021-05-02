<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver;

use Keboola\ConfigurationVariablesResolver\Configuration\SharedCodeRow;
use Keboola\ConfigurationVariablesResolver\Configuration\Variables;
use Keboola\ConfigurationVariablesResolver\Configuration\VariableValues;
use Keboola\ConfigurationVariablesResolver\Exception\UserException;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Components;
use Keboola\StorageApiBranch\ClientWrapper;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class ComponentsClientHelper
{
    public const KEBOOLA_VARIABLES = 'keboola.variables';

    public const KEBOOLA_SHARED_CODE = 'keboola.shared-code';

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
            return (new Variables())->process($vConfiguration['configuration']);
        } catch (ClientException $e) {
            throw new UserException('Variable configuration cannot be read: ' . $e->getMessage(), 400, $e);
        } catch (InvalidConfigurationException $e) {
            throw new UserException('Variable configuration is invalid: ' . $e->getMessage(), 400, $e);
        }
    }

    public function getVariablesConfigurationRow(string $variablesId, string $variableValuesId): array
    {
        try {
            $vRow = $this->getClient()->getConfigurationRow(
                self::KEBOOLA_VARIABLES,
                $variablesId,
                $variableValuesId
            );
            return (new VariableValues())->process($vRow['configuration']);
        } catch (ClientException $e) {
            throw new UserException(
                sprintf(
                    'Cannot read variable values "%s" of variables configuration "%s".',
                    $variableValuesId,
                    $variablesId,
                ),
                400,
                $e
            );
        } catch (InvalidConfigurationException $e) {
            throw new UserException('Variable values configuration is invalid: ' . $e->getMessage(), 400, $e);
        }
    }

    public function getSharedCodeConfigurationRow(string $sharedCodeId, string $sharedCodeRowId): array
    {
        try {
            $sharedCodeConfiguration = $this->getClient()->getConfigurationRow(
                self::KEBOOLA_SHARED_CODE,
                $sharedCodeId,
                $sharedCodeRowId
            );
            return (new SharedCodeRow())->process($sharedCodeConfiguration['configuration']);
        } catch (ClientException $e) {
            throw new UserException('Shared code configuration cannot be read: ' . $e->getMessage(), 400, $e);
        } catch (InvalidConfigurationException $e) {
            throw new UserException('Shared code configuration is invalid: ' . $e->getMessage(), 400, $e);
        }
    }
}
