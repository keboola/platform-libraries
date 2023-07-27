<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver;

use Keboola\StorageApi\Components;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

class SharedCodeResolver
{
    private ClientWrapper$clientWrapper;
    private LoggerInterface $logger;
    private ComponentsClientHelper $componentsHelper;

    public function __construct(ClientWrapper $clientWrapper, LoggerInterface $logger)
    {
        $this->clientWrapper = $clientWrapper;
        $this->logger = $logger;
        $this->componentsHelper = new ComponentsClientHelper(
            new Components($this->clientWrapper->getBranchClient()),
        );
    }

    public function resolveSharedCode(array $configuration): array
    {
        if (!empty($configuration['shared_code_id'])) {
            $sharedCodeId = $configuration['shared_code_id'];
        }
        if (!empty($configuration['shared_code_row_ids'])) {
            $sharedCodeRowIds = $configuration['shared_code_row_ids'];
        }
        if (empty($sharedCodeId) || empty($sharedCodeRowIds)) {
            return $configuration;
        }

        $context = new SharedCodeContext();
        foreach ($sharedCodeRowIds as $sharedCodeRowId) {
            $sharedCodeConfiguration = $this->componentsHelper->getSharedCodeConfigurationRow(
                $sharedCodeId,
                $sharedCodeRowId
            );
            $context->pushValue(
                $sharedCodeRowId,
                $sharedCodeConfiguration['code_content']
            );
        }
        $this->logger->info(sprintf(
            'Loaded shared code snippets with ids: "%s".',
            implode(', ', $context->getKeys())
        ));

        $newConfiguration = $configuration;
        $this->replaceSharedCodeInConfiguration($newConfiguration, $context);

        return $newConfiguration;
    }

    private function replaceSharedCodeInConfiguration(array &$configuration, SharedCodeContext $context): void
    {
        foreach ($configuration as &$value) {
            if (is_array($value)) {
                if ($this->isScalarOrdinalArray($value)) {
                    $value = $this->replaceSharedCodeInArray($value, $context);
                } else {
                    $this->replaceSharedCodeInConfiguration($value, $context);
                }
            } // else it's a scalar, leave as is - shared code is replaced only in arrays
        }
    }

    private function isScalarOrdinalArray(array $array): bool
    {
        foreach ($array as $key => $value) {
            if (!is_scalar($value) || !is_int($key)) {
                return false;
            }
        }
        return true;
    }

    private function replaceSharedCodeInArray(array $nodes, SharedCodeContext $context): array
    {
        $renderedNodes = [];
        foreach ($nodes as $node) {
            preg_match_all('/{{([ a-zA-Z0-9_-]+)}}/', $node, $matches, PREG_PATTERN_ORDER);
            $matches = $matches[1];
            array_walk(
                $matches,
                function (&$v) {
                    $v = trim($v);
                }
            );
            $filteredMatches = array_intersect($context->getKeys(), $matches);
            if (count($filteredMatches) === 0) {
                $renderedNodes[] = $node;
            } else {
                foreach ($filteredMatches as $match) {
                    $match = trim((string) $match);
                    $renderedNodes = array_merge($renderedNodes, $context->$match);
                }
            }
        }
        return $renderedNodes;
    }
}
