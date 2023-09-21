<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Helper;

use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\File\Options\InputFileOptions;
use Keboola\InputMapping\File\Options\RewrittenInputFileOptions;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

class RealDevStorageTagsRewriteHelper implements TagsRewriteHelperInterface
{
    public const MATCH_TYPE_EXCLUDE = 'exclude';
    public const MATCH_TYPE_INCLUDE = 'include';

    public function rewriteFileTags(
        InputFileOptions $fileConfigurationOriginal,
        ClientWrapper $clientWrapper,
        LoggerInterface $logger,
    ): RewrittenInputFileOptions {
        if (!$clientWrapper->isDevelopmentBranch()) {
            return new RewrittenInputFileOptions(
                $fileConfigurationOriginal->getDefinition(),
                $fileConfigurationOriginal->isDevBranch(),
                $fileConfigurationOriginal->getRunId(),
                $fileConfigurationOriginal->getDefinition(),
                (int) $clientWrapper->getBranchId(),
            );
        }
        $fileConfiguration = $fileConfigurationOriginal->getDefinition();

        if (!empty($fileConfiguration['tags'])) {
            if ($this->hasFilesWithTagsInBranch($clientWrapper, $fileConfiguration['tags'])) {
                $logger->info(
                    sprintf(
                        'Using files from development branch "%s" for tags "%s".',
                        $clientWrapper->getBranchId(),
                        implode(', ', $fileConfiguration['tags']),
                    ),
                );
                return new RewrittenInputFileOptions(
                    $fileConfiguration,
                    $fileConfigurationOriginal->isDevBranch(),
                    $fileConfigurationOriginal->getRunId(),
                    $fileConfigurationOriginal->getDefinition(),
                    (int) $clientWrapper->getBranchId(), // use dev branch
                );
            } else {
                $logger->info(
                    sprintf(
                        'Using files from default branch "%s" for tags "%s".',
                        $clientWrapper->getDefaultBranch()->id,
                        implode(', ', $fileConfiguration['tags']),
                    ),
                );
                return new RewrittenInputFileOptions(
                    $fileConfiguration,
                    $fileConfigurationOriginal->isDevBranch(),
                    $fileConfigurationOriginal->getRunId(),
                    $fileConfigurationOriginal->getDefinition(),
                    (int) $clientWrapper->getDefaultBranch()->id, // use prod branch
                );
            }
        }

        if (!empty($fileConfiguration['source']['tags'])) {
            $includeTags = array_filter($fileConfiguration['source']['tags'], function ($tag) {
                return $tag['match'] === self::MATCH_TYPE_INCLUDE;
            });
            if (!empty($fileConfiguration['processed_tags'])) {
                throw new InvalidInputException(
                    'The "processed_tags" property is not supported for development storage.',
                );
            }

            if ($this->hasFilesWithSourceTagsInBranch($clientWrapper, $includeTags)) {
                $logger->info(
                    sprintf(
                        'Using files from development branch "%s" for tags "%s".',
                        $clientWrapper->getBranchId(),
                        implode(', ', BuildQueryFromConfigurationHelper::getTagsFromSourceTags($includeTags)),
                    ),
                );
                return new RewrittenInputFileOptions(
                    $fileConfiguration,
                    $fileConfigurationOriginal->isDevBranch(),
                    $fileConfigurationOriginal->getRunId(),
                    $fileConfigurationOriginal->getDefinition(),
                    (int) $clientWrapper->getBranchId(),
                );
            }

            $logger->info(
                sprintf(
                    'Using files from default branch "%s" for tags "%s".',
                    $clientWrapper->getDefaultBranch()->id,
                    implode(', ', BuildQueryFromConfigurationHelper::getTagsFromSourceTags($includeTags)),
                ),
            );
            return new RewrittenInputFileOptions(
                $fileConfiguration,
                $fileConfigurationOriginal->isDevBranch(),
                $fileConfigurationOriginal->getRunId(),
                $fileConfigurationOriginal->getDefinition(),
                (int) $clientWrapper->getDefaultBranch()->id,
            );
        }
        /* at this point, nothing has changed - no tags are specified in the configuration */
        return new RewrittenInputFileOptions(
            $fileConfiguration,
            $fileConfigurationOriginal->isDevBranch(),
            $fileConfigurationOriginal->getRunId(),
            $fileConfigurationOriginal->getDefinition(),
            (int) $clientWrapper->getDefaultBranch()->id,
        );
    }

    private function hasFilesWithTagsInBranch(ClientWrapper $clientWrapper, array $tags): bool
    {
        $options = new ListFilesOptions();
        $options->setTags($tags);
        $options->setLimit(1);

        return count($clientWrapper->getBranchClient()->listFiles($options)) > 0;
    }

    private function hasFilesWithSourceTagsInBranch(ClientWrapper $clientWrapper, array $tags): bool
    {
        $options = new ListFilesOptions();
        $options->setQuery(BuildQueryFromConfigurationHelper::buildQueryForSourceTags($tags));
        $options->setLimit(1);

        return count($clientWrapper->getBranchClient()->listFiles($options)) > 0;
    }
}
