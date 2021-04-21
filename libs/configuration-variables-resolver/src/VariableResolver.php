<?php

namespace Keboola\DockerBundle\Docker;

use Keboola\DockerBundle\Docker\Configuration\Variables;
use Keboola\DockerBundle\Docker\Configuration\VariableValues;
use Keboola\DockerBundle\Docker\Runner\VariablesContext;
use Keboola\DockerBundle\Exception\UserException;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Components;
use Keboola\StorageApiBranch\ClientWrapper;
use Mustache_Engine;
use Psr\Log\LoggerInterface;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class VariableResolver
{
    /**
     * @var ClientWrapper
     */
    private $clientWrapper;

    const KEBOOLA_VARIABLES = 'keboola.variables';

    /**
     * @var Mustache_Engine
     */
    private $moustache;

    /**
     * @var LoggerInterface
     */
    private $logger;

    public function __construct(ClientWrapper $clientWrapper, LoggerInterface $logger)
    {
        $this->clientWrapper = $clientWrapper;
        $this->moustache = new Mustache_Engine([
            'escape' => function ($string) {
                return trim(json_encode($string), '"');
            },
        ]);
        $this->logger = $logger;
    }

    public function resolveVariables(array $jobDefinitions, $variableValuesId, $variableValuesData)
    {
        if ($variableValuesId && $variableValuesData) {
            throw new UserException('Only one of variableValuesId and variableValuesData can be entered.');
        }
        /** @var JobDefinition $jobDefinition */
        $newJobDefinitions = [];
        foreach ($jobDefinitions as $jobDefinition) {
            if (!empty($jobDefinition->getConfiguration()['variables_id'])) {
                $variablesId = $jobDefinition->getConfiguration()['variables_id'];
            } else {
                $variablesId = null;
            }
            if (!empty($jobDefinition->getConfiguration()['variables_values_id'])) {
                $defaultValuesId = $jobDefinition->getConfiguration()['variables_values_id'];
            } else {
                $defaultValuesId = null;
            }
            if ($variablesId) {
                if ($this->clientWrapper->hasBranch()) {
                    $components = new Components($this->clientWrapper->getBranchClient());
                } else {
                    $components = new Components($this->clientWrapper->getBasicClient());
                }
                try {
                    $vConfiguration = $components->getConfiguration(self::KEBOOLA_VARIABLES, $variablesId);
                    $vConfiguration = (new Variables())->parse(array('config' => $vConfiguration['configuration']));
                } catch (ClientException $e) {
                    throw new UserException('Variable configuration cannot be read: ' . $e->getMessage(), $e);
                } catch (InvalidConfigurationException $e) {
                    throw new UserException('Variable configuration is invalid: ' . $e->getMessage(), $e);
                }
                if ($variableValuesData) {
                    $this->logger->info('Replacing variables using inline values.');
                    $vRow = $variableValuesData;
                } elseif ($variableValuesId) {
                    $this->logger->info(sprintf('Replacing variables using values with ID: "%s".', $variableValuesId));
                    try {
                        $vRow = $components->getConfigurationRow(
                            self::KEBOOLA_VARIABLES,
                            $variablesId,
                            $variableValuesId
                        );
                        $vRow = $vRow['configuration'];
                    } catch (ClientException $e) {
                        throw new UserException(
                            sprintf(
                                'Cannot read requested variable values "%s" for configuration "%s", row "%s".',
                                $variableValuesId,
                                $jobDefinition->getConfigId(),
                                $jobDefinition->getRowId()
                            ),
                            $e
                        );
                    }
                } elseif ($defaultValuesId) {
                    $this->logger->info(
                        sprintf('Replacing variables using default values with ID: "%s"', $defaultValuesId)
                    );
                    try {
                        $vRow = $components->getConfigurationRow(
                            self::KEBOOLA_VARIABLES,
                            $variablesId,
                            $defaultValuesId
                        );
                        $vRow = $vRow['configuration'];
                    } catch (ClientException $e) {
                        throw new UserException(
                            sprintf(
                                'Cannot read default variable values "%s" for configuration "%s", row "%s".',
                                $defaultValuesId,
                                $jobDefinition->getConfigId(),
                                $jobDefinition->getRowId()
                            ),
                            $e
                        );
                    }
                } else {
                    throw new UserException(sprintf(
                        'No variable values provided for configuration "%s", row "%s", referencing variables "%s".',
                        $jobDefinition->getConfigId(),
                        $jobDefinition->getRowId(),
                        $variablesId
                    ));
                }
                try {
                    $vRow = (new VariableValues())->parse(array('config' => $vRow));
                } catch (InvalidConfigurationException $e) {
                    throw new UserException('Variable values configuration is invalid: ' . $e->getMessage(), $e);
                }
                $context = new VariablesContext($vRow);
                $variableNames = [];
                foreach ($vConfiguration['variables'] as $variable) {
                    $variableNames[] = $variable['name'];
                    if (!$context->__isset($variable['name'])) {
                        throw new UserException(sprintf('No value provided for variable "%s".', $variable['name']));
                    }
                }
                $this->logger->info(sprintf('Replaced values for variables: "%s".', implode(', ', $variableNames)));

                $newConfiguration = json_decode(
                    $this->moustache->render(json_encode($jobDefinition->getConfiguration()), $context),
                    true
                );
                if (json_last_error() !== JSON_ERROR_NONE) {
                    throw new UserException(
                        'Variable replacement resulted in invalid configuration, error: ' . json_last_error_msg()
                    );
                }
                if ($context->getMissingVariables()) {
                    throw new UserException(
                        sprintf('Missing values for placeholders: "%s"', implode(', ', $context->getMissingVariables()))
                    );
                }
                $newJobDefinitions[] = new JobDefinition(
                    $newConfiguration,
                    $jobDefinition->getComponent(),
                    $jobDefinition->getConfigId(),
                    $jobDefinition->getConfigVersion(),
                    $jobDefinition->getState(),
                    $jobDefinition->getRowId(),
                    $jobDefinition->isDisabled()
                );
            } else {
                $newJobDefinitions[] = $jobDefinition;
            }
        }
        return $newJobDefinitions;
    }
}
