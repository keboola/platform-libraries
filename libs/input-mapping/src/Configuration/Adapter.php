<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Configuration;

use Keboola\InputMapping\Exception\InputOperationException;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\SplFileInfo;
use Symfony\Component\Serializer\Encoder\JsonEncoder;
use Symfony\Component\Yaml\Yaml;

abstract class Adapter
{
    public const FORMAT_YAML = 'yaml';
    public const FORMAT_JSON = 'json';

    private array $config;
    private string $format;
    protected string $configClass = '';

    /**
     * @param self::FORMAT_YAML | self::FORMAT_JSON $format
     */
    public function __construct(string $format = self::FORMAT_JSON)
    {
        $this->validateFormat($format);
        $this->format = $format;
    }

    public function getConfig(): array
    {
        return $this->config;
    }

    public function getFormat(): string
    {
        return $this->format;
    }

    /**
     * Get configuration file suffix.
     */
    public function getFileExtension(): string
    {
        switch ($this->format) {
            case self::FORMAT_YAML:
                return '.yml';
            case self::FORMAT_JSON:
                return '.json';
            default:
                $this->throwInvalidConfigurationFormatException();
        }
    }

    private function throwInvalidConfigurationFormatException(): void
    {
        throw new InputOperationException("Invalid configuration format {$this->format}.");
    }

    /**
     * @param self::FORMAT_YAML | self::FORMAT_JSON $format
     */
    private function validateFormat(string $format): void
    {
        if (!in_array($format, [self::FORMAT_YAML, self::FORMAT_JSON])) {
            throw new InputOperationException("Configuration format '{$format}' not supported");
        }
    }

    public function setConfig(array $config): self
    {
        $className = $this->configClass;
        $this->config = (new $className())->parse(['config' => $config]);
        return $this;
    }

    public function serialize(): string
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
            $this->throwInvalidConfigurationFormatException();
        }
        return $serialized;
    }

    /**
     * Read configuration from file
     */
    public function readFromFile(string $file): array
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
            $this->throwInvalidConfigurationFormatException();
        }
        $this->setConfig((array) $data);
        return $this->getConfig();
    }

    /**
     * Write configuration to file in given format
     */
    public function writeToFile(string $file): void
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
            $this->throwInvalidConfigurationFormatException();
        }
        $fs = new Filesystem();
        $fs->dumpFile($file, $serialized);
    }

    public function getContents(string $file): string
    {
        if (!(new Filesystem())->exists($file)) {
            throw new InputOperationException(sprintf('File %s not found.', $file));
        }
        return (new SplFileInfo($file, '', basename($file)))->getContents();
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
