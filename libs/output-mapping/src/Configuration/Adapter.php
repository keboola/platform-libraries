<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Configuration;

use Keboola\OutputMapping\Exception\OutputOperationException;
use Symfony\Component\Serializer\Encoder\JsonEncoder;
use Symfony\Component\Yaml\Yaml;

class Adapter
{
    public const FORMAT_YAML = 'yaml';
    public const FORMAT_JSON = 'json';

    protected ?array $config = null;
    /** @var class-string<Configuration> */
    protected string $configClass;

    /**
     * @param self::FORMAT_YAML | self::FORMAT_JSON $format
     */
    public function __construct(
        protected readonly string $format = 'json',
    ) {
    }

    public function getConfig(): ?array
    {
        return $this->config;
    }

    /**
     * @return self::FORMAT_YAML | self::FORMAT_JSON
     */
    public function getFormat(): string
    {
        return $this->format;
    }

    public function getFileExtension(): string
    {
        switch ($this->format) {
            case self::FORMAT_YAML:
                return '.yml';
            case self::FORMAT_JSON:
                return '.json';
            default:
                throw new OutputOperationException("Invalid configuration format {$this->format}.");
        }
    }

    public function setConfig(array $config): static
    {
        $className = $this->configClass;
        $this->config = (new $className())->parse(['config' => $config]);
        return $this;
    }

    /**
     * Read configuration from data
     *
     * @return array Configuration data
     * @throws OutputOperationException
     */
    public function deserialize(string $serialized): array
    {
        if ($this->getFormat() === self::FORMAT_YAML) {
            $data = Yaml::parse($serialized);
        } elseif ($this->getFormat() === self::FORMAT_JSON) {
            $encoder = new JsonEncoder();
            $data = $encoder->decode($serialized, $encoder::FORMAT);
        } else {
            throw new OutputOperationException(sprintf('Invalid configuration format "%s".', $this->format));
        }
        $this->setConfig((array) $data);
        return (array) $this->getConfig();
    }
}
