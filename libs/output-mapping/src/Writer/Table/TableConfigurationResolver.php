<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table;

use Keboola\OutputMapping\Configuration\Adapter;
use Keboola\OutputMapping\Configuration\Table\Manifest as TableManifest;
use Keboola\OutputMapping\Configuration\Table\Manifest\Adapter as TableAdapter;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Writer\Helper\ConfigurationMerger;
use Keboola\OutputMapping\Writer\Helper\DestinationRewriter;
use Keboola\OutputMapping\Writer\Helper\PrimaryKeyHelper;
use Keboola\OutputMapping\Writer\Helper\TagsHelper;
use Keboola\OutputMapping\Writer\Table\Source\SourceInterface;
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
        private string $format = Adapter::FORMAT_JSON
    ) {
    }

    /**
     * @param Adapter::FORMAT_YAML | Adapter::FORMAT_JSON $format
     */
    public function setFormat(string $format): void
    {
        $this->format = $format;
    }

    public function resolveTableConfiguration(
        MappingSource $mappingSource,
        ?string $defaultBucket,
        array $systemMetadata
    ): array {
        $configFromManifest = [];
        $configFromMapping = [];

        if ($mappingSource->getManifestFile() !== null) {
            $configFromManifest = $this->loadTableManifest($mappingSource->getManifestFile());

            $configFromManifest['destination'] = $this->normalizeManifestDestination(
                $configFromManifest['destination'] ?? null,
                $mappingSource->getSource(),
                $defaultBucket
            );
        }

        if ($mappingSource->getMapping() !== null) {
            $configFromMapping = $mappingSource->getMapping();
            unset($configFromMapping['source']);
        }

        $config = ConfigurationMerger::mergeConfigurations($configFromManifest, $configFromMapping);

        if (!isset($config['destination'])) {
            $this->logger->warning(sprintf(
                'Source table "%s" has neither manifest file nor mapping set, ' .
                'falling back to the source name as a destination.' .
                'This behaviour was DEPRECATED and will be removed in the future.',
                $mappingSource->getSourceName()
            ));

            $config['destination'] = $this->normalizeManifestDestination(
                null,
                $mappingSource->getSource(),
                $defaultBucket
            );
        }

        if (empty($config['destination']) || !MappingDestination::isTableId($config['destination'])) {
            throw new InvalidOutputException(sprintf(
                'Failed to resolve destination for output table "%s".',
                $mappingSource->getSourceName()
            ));
        }

        $config = $this->normalizeConfig($config, $mappingSource);

        $tokenInfo = $this->clientWrapper->getBranchClientIfAvailable()->verifyToken();
        if (in_array(TableWriter::TAG_STAGING_FILES_FEATURE, $tokenInfo['owner']['features'], true)) {
            $config = TagsHelper::addSystemTags($config, $systemMetadata, $this->logger);
        }

        return $config;
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
                    $e->getMessage()
                ),
                0,
                $e
            );
        }
    }

    private function normalizeManifestDestination(
        ?string $destination,
        SourceInterface $source,
        ?string $defaultBucket
    ): string {
        if (MappingDestination::isTableId($destination)) {
            return (string) $destination;
        }

        if ($destination === null || $destination === '') {
            $destination = basename($source->getName(), '.csv');
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

    private function normalizeConfig(array $config, MappingSource $source): array
    {
        try {
            $config = (new TableManifest())->parse([$config]);
        } catch (InvalidConfigurationException $e) {
            throw new InvalidOutputException(
                sprintf(
                    'Failed to prepare mapping configuration for table %s: %s',
                    $source->getSourceName(),
                    $e->getMessage()
                ),
                0,
                $e
            );
        }

        $config['primary_key'] = PrimaryKeyHelper::normalizeKeyArray($this->logger, $config['primary_key']);

        if (!$this->clientWrapper->getClientOptionsReadOnly()->useBranchStorage()) {
            return DestinationRewriter::rewriteDestination($config, $this->clientWrapper);
        }
        return $config;
    }
}
