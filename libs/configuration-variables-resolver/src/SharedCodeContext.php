<?php

namespace Keboola\DockerBundle\Docker\Runner;

class SharedCodeContext
{
    private $values = [];

    public function pushValue($key, $value)
    {
        $this->values[$key] = $value;
    }

    public function getKeys()
    {
        return array_keys($this->values);
    }

    public function __isset($name)
    {
        return true;
    }

    public function __get($name)
    {
        if (isset($this->values[$name])) {
            return $this->values[$name];
        } else {
            return '{{ ' . $name . ' }}';
        }
    }
}
