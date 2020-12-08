<?php

namespace Keboola\OutputMapping\Configuration;

use Keboola\InputMapping\Reader\NullWorkspaceProvider;
use Keboola\InputMapping\Reader\WorkspaceProviderInterface;
use Keboola\OutputMapping\Exception\OutputOperationException;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\SplFileInfo;
use Symfony\Component\Serializer\Encoder\JsonEncoder;
use Symfony\Component\Yaml\Yaml;

class Adapter
{
    /**
     * @var array
     */
    protected $config;

    /**
     * @var string
     */
    protected $configClass = '';

    /**
     * @var string data format, 'yaml' or 'json'
     */
    protected $format;

    /**
     * Constructor.
     *
     * @param string $format Configuration file format ('yaml', 'json')
     */
    public function __construct($format = 'json')
    {
        $this->setFormat($format);
    }

    /**
     * @return array
     */
    public function getConfig()
    {
        return $this->config;
    }

    /**
     * @return string
     */
    public function getFormat()
    {
        return $this->format;
    }


    /**
     * Get configuration file suffix.
     *
     * @return string File extension.
     */
    public function getFileExtension()
    {
        switch ($this->format) {
            case 'yaml':
                return '.yml';
            case 'json':
                return '.json';
            default:
                throw new OutputOperationException("Invalid configuration format {$this->format}.");
        }
    }

    /**
     * @param $format
     * @return $this
     * @throws OutputOperationException
     */
    public function setFormat($format)
    {
        if (!in_array($format, ['yaml', 'json'])) {
            throw new OutputOperationException("Configuration format '{$format}' not supported");
        }
        $this->format = $format;
        return $this;
    }


    /**
     * @param array $config
     * @return $this
     */
    public function setConfig($config)
    {
        $className = $this->configClass;
        $this->config = (new $className())->parse(["config" => $config]);
        return $this;
    }

    /**
     * Read configuration from data
     *
     * @param string $serialized
     * @return array Configuration data
     * @throws OutputOperationException
     */
    public function deserialize($serialized)
    {
        if ($this->getFormat() == 'yaml') {
            $data = Yaml::parse($serialized);
        } elseif ($this->getFormat() == 'json') {
            $encoder = new JsonEncoder();
            $data = $encoder->decode($serialized, $encoder::FORMAT);
        } else {
            throw new OutputOperationException(sprintf('Invalid configuration format "%s".', $this->format));
        }
        $this->setConfig($data);
        return $this->getConfig();
    }
}
