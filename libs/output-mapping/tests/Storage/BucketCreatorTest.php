<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Storage;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Storage\BucketCreator;
use Keboola\OutputMapping\SystemMetadata;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Needs\NeedsDevBranch;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyInputBucket;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use ReflectionClass;
use Throwable;

class BucketCreatorTest extends AbstractTestCase
{
    #[NeedsEmptyInputBucket]
    public function testEnsureDestinationBucket(): void
    {
        $this->clientWrapper->getTableAndFileStorageClient()->dropBucket($this->emptyInputBucketId);

        $bucketCreator = new BucketCreator($this->clientWrapper);

        $destination = new MappingDestination($this->emptyInputBucketId . '.testTable');
        $systemMetadata = new SystemMetadata([
            'runId' => '123',
            'componentId' => 'test',
            'configurationId' => '456',
        ]);

        $bucketInfo = $bucketCreator->ensureDestinationBucket($destination, $systemMetadata);
        $this->assertEquals($this->emptyInputBucketId, $bucketInfo->id);
        $this->assertEquals('KBC.createdBy.component.id', $bucketInfo->metadata[0]['key']);
        $this->assertEquals('test', $bucketInfo->metadata[0]['value']);
        $this->assertEquals('KBC.createdBy.configuration.id', $bucketInfo->metadata[1]['key']);
        $this->assertEquals('456', $bucketInfo->metadata[1]['value']);
    }

    #[NeedsEmptyInputBucket]
    public function testEnsureDestinationBucketExists(): void
    {
        $this->clientWrapper->getTableAndFileStorageClient()->dropBucket($this->emptyInputBucketId);

        $bucketCreator = new BucketCreator($this->clientWrapper);

        $destination = new MappingDestination($this->emptyInputBucketId . '.testTable');
        $systemMetadata = new SystemMetadata([
            'runId' => '123',
            'componentId' => 'test',
            'configurationId' => '456',
        ]);

        $bucketCreator->ensureDestinationBucket($destination, $systemMetadata);

        // Second call - bucket already exists
        $bucketInfo = $bucketCreator->ensureDestinationBucket($destination, $systemMetadata);
        $this->assertEquals($this->emptyInputBucketId, $bucketInfo->id);
    }

    #[NeedsEmptyInputBucket]
    public function testCreateDestinationBucketRaceCondition(): void
    {
        $this->clientWrapper->getTableAndFileStorageClient()->dropBucket($this->emptyInputBucketId);

        $destination = new MappingDestination($this->emptyInputBucketId . '.testTable');
        $systemMetadata = new SystemMetadata([
            'runId' => '123',
            'componentId' => 'test',
            'configurationId' => '456',
        ]);

        // Create the bucket first to simulate race condition
        $this->clientWrapper->getTableAndFileStorageClient()->createBucket(
            $destination->getBucketName(),
            $destination->getBucketStage(),
        );

        $bucketCreator = new BucketCreator($this->clientWrapper);

        // Use reflection to call private createDestinationBucket method directly
        $reflection = new ReflectionClass($bucketCreator);
        $createMethod = $reflection->getMethod('createDestinationBucket');
        $createMethod->setAccessible(true);

        // This should NOT throw exception even though bucket already exists (race condition)
        // The race condition handling should catch the 400 error and verify bucket exists
        try {
            $createMethod->invokeArgs($bucketCreator, [$destination, $systemMetadata]);

            // If we get here, race condition was handled correctly
            $this->assertTrue(true, 'Race condition was handled correctly');

            // Verify bucket still exists and has metadata
            $bucket = $this->clientWrapper->getTableAndFileStorageClient()->getBucket($this->emptyInputBucketId);
            $this->assertEquals($this->emptyInputBucketId, $bucket['id']);
        } catch (Throwable $e) {
            $this->fail('Race condition handling failed: ' . $e->getMessage());
        }
    }

    #[NeedsEmptyInputBucket, NeedsDevBranch]
    public function testEnsureDestinationBucketDevBranch(): void
    {
        $this->clientWrapper->getTableAndFileStorageClient()->dropBucket($this->emptyInputBucketId);

        $bucketCreator = new BucketCreator($this->clientWrapper);

        $destination = new MappingDestination($this->emptyInputBucketId . '.testTable');
        $systemMetadata = new SystemMetadata([
            'runId' => '123',
            'componentId' => 'test',
            'configurationId' => '456',
        ]);

        // create bucket in main branch
        $bucketCreator->ensureDestinationBucket($destination, $systemMetadata);

        // set it to use a branch
        $this->initClient($this->devBranchId);

        $bucketCreator = new BucketCreator($this->clientWrapper);

        $expectedMessage = 'Trying to create a table in the development bucket "';
        $expectedMessage .= $this->emptyInputBucketId;
        $expectedMessage .= '" on branch "' . $this->devBranchName . '"';
        $expectedMessage .= ' (ID "' . $this->devBranchId . '"), ';
        $expectedMessage .= 'but the bucket is not assigned to any development branch.';

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage($expectedMessage);
        $bucketCreator->ensureDestinationBucket($destination, $systemMetadata);
    }
}
