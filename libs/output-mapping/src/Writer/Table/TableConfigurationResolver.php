<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table;

use Keboola\OutputMapping\Configuration\Adapter;
use Keboola\OutputMapping\Configuration\Table\Manifest as TableManifest;
use Keboola\OutputMapping\Configuration\Table\Manifest\Adapter as TableAdapter;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\Mapping\MappingFromRawConfigurationAndPhysicalDataWithManifest;
use Keboola\OutputMapping\SystemMetadata;
use Keboola\OutputMapping\Writer\Helper\ConfigurationMerger;
use Keboola\OutputMapping\Writer\Helper\DestinationRewriter;
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
     * @param Adapter::FORMAT_YAML | Adapter::FORMAT_JSON $format
     */
    public function setFormat(string $format): void
    {
        $this->format = $format;
    }

    public function resolveTableConfiguration(
        MappingFromRawConfigurationAndPhysicalDataWithManifest $source,
        ?string $defaultBucket,
        SystemMetadata $systemMetadata,
    ): MappingFromProcessedConfiguration {
        $configFromManifest = [];
        $configFromMapping = [];

        /*
         * TODO dat do separateni metody, ktera nastavi 'destination' a nic jinyho
         * - kdyz existuje manifest, tak se vezme destination z nej  a uzivatel nema moznost to zmenit
         * - kdyz existuje manifest a soucasne ma soubor jmeno typu tableid, tak se pouzije nazvev souboru jako destination  a uzivatel nema moznost to zmenit
         * - kdyz existuje manifest a soubor nema jmeno typu tableid, tak se pouzije defaultni bucket + jmeno souboru  a uzivatel nema moznost to zmenit
         *  ^^ toto se dela pred mergem, \/\/ toto se dela po mergi
         * - kdyz neexistuje manifest, tak uzivatel muze zadat vlastni destination
         * - kdyz neexistuje manifest a uzivatel nezada vlastni destination,a soubor ma jmeno typu tableid, tak se pouzije nazev souboru jako destination
         * - kdyz neexistuje manifest a uzivatel nezada vlastni destination,a soubor ma nejmeno typu tableid, tak se pouzije default bucket + jmeno souboru
         * */
        if ($source->getManifest() !== null) {
            // TODO ted uz davno vime jestli tam manifest je nebo neni, da se sem poslat rovnou
            $configFromManifest = $this->loadTableManifest($source->getManifest()->getFile());

            // TODO by mělo jit ven z toho IF, nemeelo, protoze jinak by nesel destination uzivatelem prepsat
            $configFromManifest['destination'] = $this->normalizeManifestDestination( // TODO getTableDestination
                $configFromManifest['destination'] ?? null,
                $source->getSourceName(),
                $defaultBucket,
            );
        }

        if ($source->getConfiguration() !== null) {
            $configFromMapping = $source->getConfiguration()->asArray();
            unset($configFromMapping['source']); // TODO nevim proc se tohle unsetne - v tuhle chvili je ten field k nicemu (znamena to, ze ve vysledneni neni source od uzivatele, ale ten by mel bejt stejnej jako nazev souboru, tak je otazka semu to vadi)
        }

        // TODO destination nechceme prespat tim co ma uzivatel nastaveny $configFromMapping
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
        } // TODO viz comment nahore

        // TODO tohle by by prijit od te metody, ktera nastavi destination
        if (empty($config['destination']) || !MappingDestination::isTableId($config['destination'])) {
            throw new InvalidOutputException(sprintf(
                'Failed to resolve destination for output table "%s".',
                $source->getSourceName(),
            ));
        }


        $config = $this->normalizeConfig($config, $source->getSourceName()); // TODO move upstream
        // TODO tady je ten moment kdy si muzem udelat value object

        if ($this->clientWrapper->getToken()->hasFeature(TableWriter::TAG_STAGING_FILES_FEATURE)) {
            // TODO zjistit jestli by se ta feature nedala zlikvidovat a delat to vzdycky

            // TODO v kazdym pripade by se nemel do konfigurace ted nalepit najeakej extra field, kterej ani neni ve validaci
            // muzem to pridat do validace, nechat to uzivatele zapsat, nebo ho to nenechat zapsat a systemovy prepsat a nebo to pridat do value objektu a uzivatee to nenechat menit
            $config = TagsHelper::addSystemTags($config, $systemMetadata, $this->logger);
        }

        return new MappingFromProcessedConfiguration($config, /* @TODO ? $tags,  */ $source);
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
            // toto je edge case
            return $destination;
        }

        if ($defaultBucket !== null) {
            // toto je casetej case
            return $defaultBucket . '.' . $destination;
        }

        // TODO it would be better to throw an exception, because we know for sure the $destination is not valid here,
        // but we can't do that as it may be overridden by destination from mapping
        return $destination;
    }

    private function normalizeConfig(array $config, string $sourceName): array
    {
        try {
            $config = (new TableManifest())->parse([$config]); // TODO tady se nevaliduje manifest, ale uz hotova konfigurace
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
