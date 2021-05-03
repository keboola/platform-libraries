<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver;

use Keboola\ConfigurationVariablesResolver\Configuration\SharedCodeRow;
use Keboola\ConfigurationVariablesResolver\Exception\UserException;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApiBranch\ClientWrapper;
use Mustache_Engine;
use Psr\Log\LoggerInterface;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class SharedCodeResolver
{
    private ClientWrapper$clientWrapper;

    private Mustache_Engine $moustache;

    private LoggerInterface $logger;

    private ComponentsClientHelper $componentsHelper;

    public function __construct(ClientWrapper $clientWrapper, LoggerInterface $logger)
    {
        $this->clientWrapper = $clientWrapper;
        $this->moustache = new Mustache_Engine([
            'escape' => function ($string) {
                return trim((string) json_encode($string), '"');
            },
            'strict_callables' => true,
        ]);
        $this->logger = $logger;
        $this->componentsHelper = new ComponentsClientHelper($this->clientWrapper);
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
            $context->pushValue($sharedCodeRowId, $sharedCodeConfiguration['code_content']);
        }
        $this->logger->info(sprintf(
            'Loaded shared code snippets with ids: "%s".',
            implode(', ', $context->getKeys())
        ));

        $newConfiguration = json_decode(
            $this->moustache->render((string) json_encode($configuration), $context),
            true
        );
        if (json_last_error() !== JSON_ERROR_NONE) {
            throw new UserException(
                'Shared code replacement resulted in invalid configuration, error: ' . json_last_error_msg()
            );
        }

        return $newConfiguration;
    }
}
