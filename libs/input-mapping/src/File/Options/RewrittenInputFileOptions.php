<?php

declare(strict_types=1);

namespace Keboola\InputMapping\File\Options;

use Keboola\InputMapping\Exception\FileNotFoundException;
use Keboola\InputMapping\Helper\BuildQueryFromConfigurationHelper;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\State\InputFileStateList;
use Keboola\InputMapping\Table\Options\InputTableOptions;
use Keboola\StorageApi\Options\ListFilesOptions;

class RewrittenInputFileOptions extends InputFileOptions
{
    private array $originalDefinition;
    private int $sourceBranchId;

    public function __construct(
        array $definition,
        bool $isDevBranch,
        string $runId,
        array $originalDefinition,
        int $sourceBranchId,
    ) {
        parent::__construct($definition, $isDevBranch, $runId);
        $this->originalDefinition = $originalDefinition;
        $this->sourceBranchId = $sourceBranchId;
    }

    public function getSourceBranchId(): int
    {
        return $this->sourceBranchId;
    }

    public function getFileConfigurationIdentifier(): array
    {
        return (isset($this->originalDefinition['tags']))
            ? BuildQueryFromConfigurationHelper::getSourceTagsFromTags($this->originalDefinition['tags'])
            : ($this->originalDefinition['source']['tags'] ?? []);
    }

    public function getStorageApiFileListOptions(InputFileStateList $states): ListFilesOptions
    {
        $options = new ListFilesOptions();

        if (!empty($this->definition['filter_by_run_id'])) {
            $options->setRunId(Reader::getParentRunId($this->runId));
        }
        if (isset($this->definition['tags']) && count($this->definition['tags'])) {
            $options->setTags($this->definition['tags']);
        }
        if (isset($this->definition['query']) || isset($this->definition['source']['tags'])) {
            $options->setQuery(
                BuildQueryFromConfigurationHelper::buildQuery($this->definition)
            );
        } elseif (isset($this->definition['changed_since'])
            && $this->definition['changed_since'] !== InputTableOptions::ADAPTIVE_INPUT_MAPPING_VALUE) {
            // need to set the query for the case where query nor source tags are present, but changed_since is
            $options->setQuery(BuildQueryFromConfigurationHelper::getChangedSinceQueryPortion(
                $this->definition['changed_since']
            ));
        }
        $options->setLimit($this->definition['limit']);

        if (isset($this->definition['changed_since'])
            && $this->definition['changed_since'] === InputTableOptions::ADAPTIVE_INPUT_MAPPING_VALUE
        ) {
            try {
                // apply the state configuration limits
                $options->setSinceId(
                    $states->getFile($this->getFileConfigurationIdentifier())->getLastImportId()
                );
            } catch (FileNotFoundException) {
                // intentionally blank, no state configuration
            }
        }
        return $options;
    }
}
