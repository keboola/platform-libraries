<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table;

use Keboola\OutputMapping\Configuration\Adapter;
use Keboola\OutputMapping\Configuration\Table\Configuration as TableConfiguration;
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
        array $mappingFromManifest,
        array $mappingFromConfiguration,
        SystemMetadata $systemMetadata,
    ): array {
        $config = ConfigurationMerger::mergeConfigurations($mappingFromManifest, $mappingFromConfiguration);

        $config['destination'] = $this->ensureConfigurationDestination(
            $defaultBucket,
            $source->getSourceName(),
            $mappingFromManifest['destination'] ?? null,
            $mappingFromConfiguration['destination'] ?? null,
        );
        if (isset($config['primary_key'])) {
            $config['primary_key'] = PrimaryKeyHelper::normalizeKeyArray($this->logger, $config['primary_key']);
        }
        if (isset($config['distribution_key'])) {
            $config['distribution_key'] = PrimaryKeyHelper::normalizeKeyArray($this->logger, $config['distribution_key']);
        }

        if ($this->clientWrapper->getToken()->hasFeature(TableWriter::TAG_STAGING_FILES_FEATURE)) { // TODO vzít z classy RawConfiguratioin
            $config = TagsHelper::addSystemTags($config, $systemMetadata, $this->logger);
        }

        try {
            return (new TableConfiguration())->parse([$config]);
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

    private function ensureConfigurationDestination(
        ?string $defaultBucket,
        string $sourceName,
        ?string $destinationFromManifest = null,
        ?string $destinationFromMapping = null,
    ): string {

        if ($destinationFromMapping) {
            if (!MappingDestination::isTableId($destinationFromMapping)) {
                throw new InvalidOutputException(sprintf(
                    'Failed to resolve destination for output table "%s".',
                    $sourceName,
                ));
            }
            return $destinationFromMapping;
        }

        return $this->resolveDestinationName(
            $destinationFromManifest,
            $sourceName,
            $defaultBucket,
        );
    }

    private function resolveDestinationName(
        ?string $destination,
        string $sourceName,
        ?string $defaultBucket,
    ): string {
        $originalDestination = $destination;
        if ($destination === null || $destination === '') {
            $destination = basename($sourceName, '.csv');
        }

        if (MappingDestination::isTableId($destination)) {
            if ($originalDestination === null || $originalDestination === '') {
                $this->logger->warning(sprintf(
                    'Source table "%s" has neither manifest file nor mapping set, ' .
                    'falling back to the source name as a destination.' .
                    'This behaviour was DEPRECATED and will be removed in the future.',
                    $sourceName,
                ));
            }
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
