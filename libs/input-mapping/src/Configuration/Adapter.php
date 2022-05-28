<?php

namespace Keboola\InputMapping\Configuration;

use Keboola\InputMapping\Exception\InputOperationException;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\SplFileInfo;
use Symfony\Component\Serializer\Encoder\JsonEncoder;
use Symfony\Component\Yaml\Yaml;

class Adapter
{
    public const FORMAT_YAML = 'yaml';
    public const FORMAT_JSON = 'json';

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
    public function __construct($format = self::FORMAT_JSON)
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
            case self::FORMAT_YAML:
                return '.yml';
            case self::FORMAT_JSON:
                return '.json';
            default:
                throw new InputOperationException("Invalid configuration format {$this->format}.");
        }
    }

    /**
     * @param $format
     * @return $this
     * @throws InputOperationException
     */
    public function setFormat($format)
    {
        if (!in_array($format, [self::FORMAT_YAML, self::FORMAT_JSON])) {
            throw new InputOperationException("Configuration format '{$format}' not supported");
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

    public function serialize()
    {
        if ($this->isYamlFormat()) {
            $serialized = Yaml::dump($this->getConfig(), 10);
            if ($serialized === 'null') {
                $serialized = '{}';
            }
        } elseif ($this->isJsonFormat()) {
            $encoder = new JsonEncoder();
            $serialized = $encoder->encode(
                $this->getConfig(),
                $encoder::FORMAT,
                ['json_encode_options' => JSON_PRETTY_PRINT]
            );
        } else {
            throw new InputOperationException("Invalid configuration format {$this->format}.");
        }
        return $serialized;
    }

    /**
     *
     * Read configuration from file
     *
     * @param $file
     * @return array
     * @throws InputOperationException
     */
    public function readFromFile($file)
    {
        $fs = new Filesystem();
        if (!$fs->exists($file)) {
            throw new InputOperationException("File '$file' not found.");
        }

        $serialized = $this->getContents($file);

        if ($this->isYamlFormat()) {
            $data = Yaml::parse($serialized);
        } elseif ($this->isJsonFormat()) {
            $encoder = new JsonEncoder();
            $data = $encoder->decode($serialized, $encoder::FORMAT);
        } else {
            throw new InputOperationException("Invalid configuration format {$this->format}.");
        }
        $this->setConfig($data);
        return $this->getConfig();
    }

    /**
     *
     * Write configuration to file in given format
     *
     * @param $file
     */
    public function writeToFile($file)
    {
        if ($this->isYamlFormat()) {
            $serialized = Yaml::dump($this->getConfig(), 10);
            if ($serialized === 'null') {
                $serialized = '{}';
            }
        } elseif ($this->isJsonFormat()) {
            $encoder = new JsonEncoder();
            $serialized = $encoder->encode(
                $this->getConfig(),
                $encoder::FORMAT,
                ['json_encode_options' => JSON_PRETTY_PRINT]
            );
        } else {
            throw new InputOperationException("Invalid configuration format {$this->format}.");
        }
        $fs = new Filesystem();
        $fs->dumpFile($file, $serialized);
    }

    /**
     * @param $file
     * @return mixed
     * @throws InputOperationException
     */
    public function getContents($file)
    {
        if (!(new Filesystem())->exists($file)) {
            throw new InputOperationException("File" . $file . " not found.");
        }
        $fileHandler = new SplFileInfo($file, "", basename($file));
        if ($fileHandler) {
            return $fileHandler->getContents();
        } else {
            throw new InputOperationException("File" . $file . " not found.");
        }
    }

    private function isYamlFormat(): bool
    {
        return $this->getFormat() === 'yaml';
    }

    private function isJsonFormat(): bool
    {
        return $this->getFormat() === 'json';
    }
}
