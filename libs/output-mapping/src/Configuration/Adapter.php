<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Configuration;

use Keboola\OutputMapping\Exception\OutputOperationException;
use Keboola\StagingProvider\Staging\File\FileFormat;
use Symfony\Component\Serializer\Encoder\JsonEncoder;
use Symfony\Component\Yaml\Yaml;

class Adapter
{
    protected ?array $config = null;
    /** @var class-string<Configuration> */
    protected string $configClass;

    public function __construct(
        protected readonly FileFormat $format = FileFormat::Json,
    ) {
    }

    public function getConfig(): ?array
    {
        return $this->config;
    }

    public function getFormat(): FileFormat
    {
        return $this->format;
    }

    public function getFileExtension(): string
    {
        return $this->format->getFileExtension();
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
        $data = match ($this->format) {
            FileFormat::Yaml => Yaml::parse($serialized),
            FileFormat::Json => (new JsonEncoder)->decode($serialized, JsonEncoder::FORMAT),
        };

        $this->setConfig((array) $data);
        return (array) $this->getConfig();
    }
}
