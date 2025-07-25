<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Storage;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\SystemMetadata;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApiBranch\ClientWrapper;

class BucketCreator
{
    public function __construct(
        private readonly ClientWrapper $clientWrapper,
    ) {
    }

    /**
     * @param MappingDestination $destination
     * @param SystemMetadata $systemMetadata
     * @throws ClientException
     */
    public function ensureDestinationBucket(MappingDestination $destination, SystemMetadata $systemMetadata): BucketInfo
    {
        $destinationBucketId = $destination->getBucketId();
        try {
            $destinationBucketDetails = $this->clientWrapper->getTableAndFileStorageClient()->getBucket(
                $destinationBucketId,
            );
            $this->checkDevBucketMetadata($destination);
        } catch (ClientException $e) {
            if ($e->getCode() !== 404) {
                throw $e;
            }
            // bucket doesn't exist so we need to create it
            $this->createDestinationBucket($destination, $systemMetadata);
            $destinationBucketDetails = $this->clientWrapper->getTableAndFileStorageClient()->getBucket(
                $destinationBucketId,
            );
        }

        return new BucketInfo($destinationBucketDetails);
    }

    private function checkDevBucketMetadata(MappingDestination $destination): void
    {
        if (!$this->clientWrapper->isDevelopmentBranch()) {
            return;
        }
        $bucketId = $destination->getBucketId();
        $metadata = new Metadata($this->clientWrapper->getTableAndFileStorageClient());
        try {
            foreach ($metadata->listBucketMetadata($bucketId) as $metadatum) {
                if (($metadatum['key'] === SystemMetadata::KBC_LAST_UPDATED_BY_BRANCH_ID) ||
                    ($metadatum['key'] === SystemMetadata::KBC_CREATED_BY_BRANCH_ID)) {
                    if ((string) $metadatum['value'] === $this->clientWrapper->getBranchId()) {
                        return;
                    }

                    throw new InvalidOutputException(sprintf(
                        'Trying to create a table in the development bucket "%s" on branch ' .
                        '"%s" (ID "%s"). The bucket metadata marks it as assigned to branch with ID "%s".',
                        $bucketId,
                        $this->clientWrapper->getBranchName(),
                        $this->clientWrapper->getBranchId(),
                        $metadatum['value'],
                    ));
                }
            }
        } catch (ClientException $e) {
            // this is Ok, if the bucket it does not exists, it can't have wrong metadata
            if ($e->getCode() === 404) {
                return;
            }

            throw $e;
        }
        throw new InvalidOutputException(sprintf(
            'Trying to create a table in the development ' .
            'bucket "%s" on branch "%s" (ID "%s"), but the bucket is not assigned to any development branch.',
            $bucketId,
            $this->clientWrapper->getBranchName(),
            $this->clientWrapper->getBranchId(),
        ));
    }

    private function createDestinationBucket(MappingDestination $destination, SystemMetadata $systemMetadata): void
    {
        try {
            $this->clientWrapper->getTableAndFileStorageClient()->createBucket(
                $destination->getBucketName(),
                $destination->getBucketStage(),
            );
        } catch (ClientException $e) {
            $this->handleCreateBucketError($e, $destination);
        }

        $metadataClient = new Metadata($this->clientWrapper->getTableAndFileStorageClient());
        $metadataClient->postBucketMetadata(
            $destination->getBucketId(),
            SystemMetadata::SYSTEM_METADATA_PROVIDER,
            $systemMetadata->getCreatedMetadata(),
        );
    }

    private function handleCreateBucketError(ClientException $e, MappingDestination $destination): void
    {
        // Handle race condition when multiple parallel processes try to create the same bucket
        if ($e->getCode() === 400) {
            try {
                $this->clientWrapper->getTableAndFileStorageClient()->getBucket(
                    $destination->getBucketId(),
                );

                return;
            } catch (ClientException $getBucketException) {
                // If getBucket fails, the original createBucket error was not about bucket existing
                // Fall through to throw InvalidOutputException below
            }
        }

        throw new InvalidOutputException(
            sprintf(
                'Cannot create bucket "%s" in Storage API: %s',
                $destination->getBucketName(),
                json_encode((array) $e->getContextParams()),
            ),
            $e->getCode(),
            $e,
        );
    }
}
