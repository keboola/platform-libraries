<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Storage;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\SystemMetadata;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use Keboola\OutputMapping\Writer\TableWriter;
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
                if (($metadatum['key'] === TableWriter::KBC_LAST_UPDATED_BY_BRANCH_ID) ||
                    ($metadatum['key'] === TableWriter::KBC_CREATED_BY_BRANCH_ID)) {
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
        $this->clientWrapper->getTableAndFileStorageClient()->createBucket(
            $destination->getBucketName(),
            $destination->getBucketStage(),
        );

        $metadataClient = new Metadata($this->clientWrapper->getTableAndFileStorageClient());
        $metadataClient->postBucketMetadata(
            $destination->getBucketId(),
            TableWriter::SYSTEM_METADATA_PROVIDER,
            $systemMetadata->getCreatedMetadata(),
        );
    }



}
