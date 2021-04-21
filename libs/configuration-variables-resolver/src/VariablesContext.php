<?php

namespace Keboola\DockerBundle\Docker\Runner;

class VariablesContext
{
    private $missingVariables;
    private $values;

    public function __construct(array $configurationRow)
    {
        $this->values = [];
        foreach ($configurationRow['values'] as $row) {
            $this->values[$row['name']] = $row['value'];
        }
        $this->missingVariables = [];
    }

    public function __isset($name)
    {
        if (isset($this->values[$name])) {
            return true;
        } else {
            $this->missingVariables[] = $name;
            return false;
        }
    }

    public function __get($name)
    {
        return $this->values[$name];
    }

    public function getMissingVariables()
    {
        return $this->missingVariables;
    }
}
