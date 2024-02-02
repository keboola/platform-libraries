<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Mapping;

use Keboola\OutputMapping\Configuration\Adapter;
use Keboola\OutputMapping\Configuration\Table\Manifest as TableManifest;
use Keboola\OutputMapping\Configuration\Table\Manifest\Adapter as TableAdapter;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\SystemMetadata;
use Keboola\OutputMapping\Writer\Helper\ConfigurationMerger;
use Keboola\OutputMapping\Writer\Helper\DestinationRewriter;
use Keboola\OutputMapping\Writer\Helper\PrimaryKeyHelper;
use Keboola\OutputMapping\Writer\Helper\TagsHelper;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\Finder\SplFileInfo;

class MappingConfigurationResolver
{
    /**
     * @param Adapter::FORMAT_YAML | Adapter::FORMAT_JSON $format
     */
    public function __construct(
        private readonly ClientWrapper $clientWrapper,
        private readonly LoggerInterface $logger,
        private readonly string $format = Adapter::FORMAT_JSON,
    ) {
    }

    public function resolveTableConfiguration(
        MappingFromRawConfigurationAndPhysicalDataWithManifest $source,
        ?string $defaultBucket,
        SystemMetadata $systemMetadata,
    ): MappingFromProcessedConfiguration {
        $configFromManifest = [];
        $configFromMapping = [];

        if ($source->getManifest() !== null) {
            $configFromManifest = $this->loadTableManifest($source->getManifest()->getFile());

            $configFromManifest['destination'] = $this->normalizeManifestDestination(
                $configFromManifest['destination'] ?? null,
                $source->getSourceName(),
                $defaultBucket,
            );
        }

        if ($source->getConfiguration() !== null) {
            $configFromMapping = $source->getConfiguration()->asArray();
            unset($configFromMapping['source']);
        }

        $config = ConfigurationMerger::mergeConfigurations($configFromManifest, $configFromMapping);

        if (!isset($config['destination'])) {
            $this->logger->warning(sprintf(
                'Source table "%s" has neither manifest file nor mapping set, ' .
                'falling back to the source name as a destination.' .
                'This behaviour was DEPRECATED and will be removed in the future.',
                $source->getSourceName(),
            ));

            $config['destination'] = $this->normalizeManifestDestination(
                null,
                $source->getSourceName(),
                $defaultBucket,
            );
        }

        if (empty($config['destination']) || !MappingDestination::isTableId($config['destination'])) {
            throw new InvalidOutputException(sprintf(
                'Failed to resolve destination for output table "%s".',
                $source->getSourceName(),
            ));
        }

        $config = $this->normalizeConfig($config, $source->getSourceName());
        if ($this->clientWrapper->getToken()->hasFeature(TableWriter::TAG_STAGING_FILES_FEATURE)) {
            $config = TagsHelper::addSystemTags($config, $systemMetadata, $this->logger);
        }

        return new MappingFromProcessedConfiguration($config, $source);
    }

    private function loadTableManifest(SplFileInfo $manifestFile): array
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

    private function normalizeManifestDestination(
        ?string $destination,
        string $sourceName,
        ?string $defaultBucket,
    ): string {
        if (MappingDestination::isTableId($destination)) {
            return (string) $destination;
        }

        if ($destination === null || $destination === '') {
            $destination = basename($sourceName, '.csv');
        }

        if (MappingDestination::isTableId($destination)) {
            return $destination;
        }

        if ($defaultBucket !== null) {
            return $defaultBucket . '.' . $destination;
        }

        // it would be better to throw an exception, because we know for sure the $destination is not valid here,
        // but we can't do that as it may be overridden by destination from mapping
        return $destination;
    }

    private function normalizeConfig(array $config, string $sourceName): array
    {
        try {
            $config = (new TableManifest())->parse([$config]);
        } catch (InvalidConfigurationException $e) {
            throw new InvalidOutputException(
                sprintf(
                    'Failed to prepare mapping configuration for table %s: %s',
                    $sourceName,
                    $e->getMessage(),
                ),
                0,
                $e,
            );
        }

        $config['primary_key'] = PrimaryKeyHelper::normalizeKeyArray($this->logger, $config['primary_key']);

        // TODO Move this to BranchResolver
        if (!$this->clientWrapper->getClientOptionsReadOnly()->useBranchStorage()) {
            return DestinationRewriter::rewriteDestination($config, $this->clientWrapper);
        }
        return $config;
    }

}
