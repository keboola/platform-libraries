<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver\JobDefinition;

use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class JobDefinition
{
    /**
     * @var
     */
    private $configId;

    /**
     * @var string
     */
    private $rowId;

    /**
     * @var string
     */
    private $configVersion;

    /**
     * @var Component
     */
    private $component;

    /**
     * @var array
     */
    private $configuration = [];

    /**
     * @var array
     */
    private $state = [];

    /**
     * @var bool
     */
    private $isDisabled = false;

    /**
     * JobDefinition constructor.
     *
     * @param array $configuration
     * @param Component $component
     * @param string $configId
     * @param string $configVersion
     * @param array $state
     * @param string $rowId
     * @param bool $isDisabled
     */
    public function __construct(array $configuration, Component $component, $configId = null, $configVersion = null, array $state = [], $rowId = null, $isDisabled = false)
    {
        $this->configuration = $this->normalizeConfiguration($configuration);
        $this->component = $component;
        $this->configId = $configId;
        $this->configVersion = $configVersion;
        $this->rowId = $rowId;
        $this->isDisabled = $isDisabled;
        $this->state = $state;

        return $this;
    }

    /**
     * @return string
     */
    public function getComponentId()
    {
        return $this->component ? $this->component->getId() : null;
    }

    /**
     * @return mixed
     */
    public function getConfigId()
    {
        return $this->configId;
    }

    /**
     * @return string
     */
    public function getRowId()
    {
        return $this->rowId;
    }

    /**
     * @return string
     */
    public function getConfigVersion()
    {
        return $this->configVersion;
    }

    /**
     * @return Component
     */
    public function getComponent()
    {
        return $this->component;
    }

    /**
     * @return array
     */
    public function getConfiguration()
    {
        return $this->configuration;
    }

    /**
     * @return array
     */
    public function getState()
    {
        return $this->state;
    }

    /**
     * @return bool
     */
    public function isDisabled()
    {
        return $this->isDisabled;
    }

    private function normalizeConfiguration($configuration)
    {
        try {
            $configuration = (new Configuration\Container())->parse(['container' => $configuration]);
        } catch (InvalidConfigurationException $e) {
            throw new UserException($e->getMessage(), $e);
        }
        $configuration['storage'] = empty($configuration['storage']) ? [] : $configuration['storage'];
        $configuration['processors'] = empty($configuration['processors']) ? [] : $configuration['processors'];
        $configuration['parameters'] = empty($configuration['parameters']) ? [] : $configuration['parameters'];

        return $configuration;
    }
}
