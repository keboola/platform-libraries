<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Staging;

use Keboola\InputMapping\Exception\StagingException;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

abstract class AbstractStrategyFactory
{
    public const ABS = 'abs';
    public const LOCAL = 'local';
    public const S3 = 's3';
    public const WORKSPACE_ABS = 'workspace-abs';
    public const WORKSPACE_REDSHIFT = 'workspace-redshift';
    public const WORKSPACE_SNOWFLAKE = 'workspace-snowflake';
    public const WORKSPACE_SYNAPSE = 'workspace-synapse';
    public const WORKSPACE_EXASOL = 'workspace-exasol';
    public const WORKSPACE_TERADATA = 'workspace-teradata';
    public const WORKSPACE_BIGQUERY = 'workspace-bigquery';

    public function __construct(
        protected readonly ClientWrapper $clientWrapper,
        protected readonly LoggerInterface $logger,
        protected readonly string $format,
    ) {
    }

    /**
     * @return AbstractStagingDefinition[]
     */
    abstract public function getStrategyMap(): array;

    /**
     * @param Scope[] $scopes
     */
    public function addProvider(ProviderInterface $provider, array $scopes): void
    {
        foreach ($scopes as $stagingType => $scope) {
            if (!isset($this->getStrategyMap()[$stagingType])) {
                throw new StagingException(sprintf(
                    'Staging "%s" is unknown. Known types are "%s".',
                    $stagingType,
                    implode(', ', array_keys($this->getStrategyMap())),
                ));
            }
            $staging = $this->getStrategyMap()[$stagingType];
            foreach ($scope->getScopeTypes() as $scopeType) {
                switch ($scopeType) {
                    case Scope::TABLE_DATA:
                        $staging->setTableDataProvider($provider);
                        break;
                    case Scope::TABLE_METADATA:
                        $staging->setTableMetadataProvider($provider);
                        break;
                    case Scope::FILE_DATA:
                        $staging->setFileDataProvider($provider);
                        break;
                    case Scope::FILE_METADATA:
                        $staging->setFileMetadataProvider($provider);
                        break;
                    default:
                        throw new StagingException(sprintf('Invalid scope type: "%s". ', $scopeType));
                }
            }
        }
    }

    public function getLogger(): LoggerInterface
    {
        return $this->logger;
    }

    public function getClientWrapper(): ClientWrapper
    {
        return $this->clientWrapper;
    }

    abstract protected function getStagingDefinition(string $stagingType): AbstractStagingDefinition;
}
