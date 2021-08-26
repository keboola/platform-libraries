<?php

namespace Keboola\OutputMapping\Writer\Table;

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
    /** @var ClientWrapper */
    private $clientWrapper;

    /** @var LoggerInterface */
    private $logger;

    /** @var string */
    private $format;

    /**
     * @param string $format
     */
    public function __construct(ClientWrapper $clientWrapper, LoggerInterface $logger, $format = 'json')
    {
        $this->clientWrapper = $clientWrapper;
        $this->logger = $logger;
        $this->format = $format;
    }

    /**
     * @param string $format
     */
    public function setFormat($format)
    {
        $this->format = $format;
    }

    /**
     * @param null|string $defaultBucket
     * @return array
     * @throws InvalidOutputException
     */
    public function resolveTableConfiguration(MappingSource $source, $defaultBucket, array $systemMetadata)
    {
        $configFromManifest = [];
        $configFromMapping = [];

        if ($source->getManifestFile() !== null) {
            $configFromManifest = $this->loadTableManifest($source->getManifestFile());

            $configFromManifest['destination'] = $this->normalizeManifestDestination(
                isset($configFromManifest['destination']) ? $configFromManifest['destination'] : null,
                $source->getSource(),
                $defaultBucket
            );
        }

        if ($source->getMapping() !== null) {
            $configFromMapping = $source->getMapping();
            unset($configFromMapping['source']);
        }

        $config = ConfigurationMerger::mergeConfigurations($configFromManifest, $configFromMapping);

        if (empty($config['destination'])) {
            throw new InvalidOutputException(sprintf(
                'Failed to resolve destination for output table "%s".',
                $source->getSourceName()
            ));
        }

        $config = $this->normalizeConfig($config, $source);

        $tokenInfo = $this->clientWrapper->getBasicClient()->verifyToken();
        if (in_array(TableWriter::TAG_STAGING_FILES_FEATURE, $tokenInfo['owner']['features'], true)) {
            $config = TagsHelper::addSystemTags($config, $systemMetadata, $this->logger);
        }

        return $config;
    }

    /**
     * @return array
     * @throws InvalidOutputException
     */
    private function loadTableManifest(SplFileInfo $manifestFile)
    {
        $adapter = new TableAdapter($this->format);

        try {
            return $adapter->deserialize($manifestFile->getContents());
        } catch (InvalidConfigurationException $e) {
            throw new InvalidOutputException(
                sprintf('Failed to read table manifest from file %s %s', $manifestFile->getBasename(), $e->getMessage()),
                0,
                $e
            );
        }
    }

    /**
     * @param null|string $destination
     * @param SourceInterface $source
     * @param null|string $defaultBucket
     * @return string
     */
    private function normalizeManifestDestination($destination, $source, $defaultBucket)
    {
        if (MappingDestination::isTableId($destination)) {
            return $destination;
        }

        if ($destination === null || $destination === '') {
            $destination = basename($source->getName(), '.csv');
        }

        if ($defaultBucket !== null) {
            return $defaultBucket . '.' . $destination;
        }

        // it would be better to throw an exception, because we know for sure the $destination is not valid here,
        // but we can't do that as it may be overridden by destination from mapping
        return $destination;
    }

    /**
     * @param array $config
     * @param MappingSource $source
     * @return array
     */
    private function normalizeConfig(array $config, MappingSource $source)
    {
        try {
            $config = (new TableManifest())->parse([$config]);
        } catch (InvalidConfigurationException $e) {
            throw new InvalidOutputException(
                sprintf("Failed to prepare mapping configuration for table %s: %s", $source->getSourceName(), $e->getMessage()),
                0,
                $e
            );
        }

        $config['primary_key'] = PrimaryKeyHelper::normalizeKeyArray($this->logger, $config['primary_key']);

        return DestinationRewriter::rewriteDestination($config, $this->clientWrapper);
    }
}
