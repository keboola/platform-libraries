<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table;

use Keboola\OutputMapping\Configuration\Adapter;
use Keboola\OutputMapping\Configuration\Table\Manifest as TableManifest;
use Keboola\OutputMapping\Configuration\Table\Manifest\Adapter as TableAdapter;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Mapping\MappingFromRawConfigurationAndPhysicalDataWithManifest;
use Keboola\OutputMapping\SystemMetadata;
use Keboola\OutputMapping\Writer\Helper\ConfigurationMerger;
use Keboola\OutputMapping\Writer\Helper\PrimaryKeyHelper;
use Keboola\OutputMapping\Writer\Helper\TagsHelper;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\Finder\SplFileInfo;

class TableConfigurationResolver
{
    /**
     * @param Adapter::FORMAT_YAML | Adapter::FORMAT_JSON $format
     */
    public function __construct(
        private readonly ClientWrapper $clientWrapper,
        private readonly LoggerInterface $logger,
        private string $format = Adapter::FORMAT_JSON,
    ) {
    }

    /**
     * @param string $format
     */
    public function setFormat(string $format): void
    {
        $this->format = $format;
    }

    public function resolveTableConfiguration(
        ?string $defaultBucket,
        MappingFromRawConfigurationAndPhysicalDataWithManifest $source,
        ?array $configFromManifest,
        array $configFromMapping,
        SystemMetadata $systemMetadata,
    ): array {
        $config = ConfigurationMerger::mergeConfigurations($configFromManifest ?? [], $configFromMapping);
        $config['destination'] = $this->ensureConfigurationDestination(
            $defaultBucket,
            $source->getSourceName(),
            $configFromManifest === null,
            $configFromManifest['destination'] ?? null,
            $configFromMapping['destination'] ?? null,
        );
        $config['primary_key'] = PrimaryKeyHelper::normalizeKeyArray($this->logger, $config['primary_key']);

        if ($this->clientWrapper->getToken()->hasFeature(TableWriter::TAG_STAGING_FILES_FEATURE)) {
            $config = TagsHelper::addSystemTags($config, $systemMetadata, $this->logger);
        }

        try {
            return (new TableManifest())->parse([$config]); // TODO tady se nevaliduje manifest, ale uz hotova konfigurace
        } catch (InvalidConfigurationException $e) {
            throw new InvalidOutputException(
                sprintf(
                    'Failed to prepare mapping configuration for table %s: %s',
                    $source->getSourceName(),
                    $e->getMessage(),
                ),
                0,
                $e,
            );
        }
    }

    public function loadTableManifest(SplFileInfo $manifestFile): array
    {
        $adapter = new TableAdapter($this->format);

        try {
            return $adapter->deserialize($manifestFile->getContents());
        } catch (InvalidConfigurationException $e) {
            throw new InvalidOutputException(
                sprintf(
                    'Failed to read table manifest from file %s %s',
                    $manifestFile->getBasename(),
                    $e->getMessage(),
                ),
                0,
                $e,
            );
        }
    }

    private function ensureConfigurationDestination(
        ?string $defaultBucket,
        string $sourceName,
        bool $hasManifest,
        ?string $destinationFromManifest = null,
        ?string $destinationFromMapping = null,
    ): string {

        if ($destinationFromMapping) {
            return $destinationFromMapping;
        }

        if ($hasManifest) {
            return $this->resolveDestinationName(
                $destinationFromManifest,
                $sourceName,
                $defaultBucket,
            );
        }

        $this->logger->warning(sprintf(
            'Source table "%s" has neither manifest file nor mapping set, ' .
            'falling back to the source name as a destination.' .
            'This behaviour was DEPRECATED and will be removed in the future.',
            $sourceName,
        ));

        return $this->resolveDestinationName(
            null,
            $sourceName,
            $defaultBucket,
        );
    }

    private function resolveDestinationName(
        ?string $destination,
        string $sourceName,
        ?string $defaultBucket,
    ): string {
        if ($destination === null || $destination === '') {
            $destination = basename($sourceName, '.csv');
        }

        if (MappingDestination::isTableId($destination)) {
            return (string) $destination;
        }

        if ($defaultBucket !== null) {
            return $defaultBucket . '.' . $destination;
        }

        throw new InvalidOutputException(sprintf(
            'Failed to resolve destination for output table "%s".',
            $sourceName,
        ));
    }
}
