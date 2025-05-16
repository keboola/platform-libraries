<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Configuration;

use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\StagingProvider\Staging\File\FileFormat;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\SplFileInfo;
use Symfony\Component\Serializer\Encoder\JsonEncoder;
use Symfony\Component\Yaml\Yaml;

abstract class Adapter
{
    private ?array $config = null;
    /** @var class-string<Configuration> */
    protected string $configClass;

    public function __construct(
        private readonly FileFormat $format = FileFormat::Json,
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

    public function setConfig(array $config): self
    {
        $className = $this->configClass;
        $this->config = (new $className())->parse(['config' => $config]);
        return $this;
    }

    public function serialize(): string
    {
        return match ($this->format) {
            FileFormat::Yaml => $this->serializeYaml($this->getConfig()),
            FileFormat::Json => $this->serializeJson($this->getConfig()),
        };
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

        $data = match ($this->format) {
            FileFormat::Yaml => Yaml::parse($serialized),
            FileFormat::Json => (new JsonEncoder())->decode($serialized, JsonEncoder::FORMAT),
        };
        $this->setConfig((array) $data);
        return (array) $this->getConfig();
    }

    /**
     * Write configuration to file in given format
     */
    public function writeToFile(string $file): void
    {
        $fs = new Filesystem();
        $fs->dumpFile($file, $this->serialize());
    }

    public function getContents(string $file): string
    {
        if (!(new Filesystem())->exists($file)) {
            throw new InputOperationException(sprintf('File %s not found.', $file));
        }
        return (new SplFileInfo($file, '', basename($file)))->getContents();
    }

    private function serializeYaml(?array $data): string
    {
        $serialized = Yaml::dump($data, 10);

        if ($serialized === 'null') {
            $serialized = '{}';
        }

        return $serialized;
    }

    private function serializeJson(?array $data): string
    {
        $encoder = new JsonEncoder();
        return $encoder->encode(
            $data,
            JsonEncoder::FORMAT,
            ['json_encode_options' => JSON_PRETTY_PRINT],
        );
    }
}
