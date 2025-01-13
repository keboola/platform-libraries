<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Needs;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\Temp\Temp;
use ReflectionAttribute;
use ReflectionObject;

class TestSatisfyer
{
    /**
     * @param class-string $attribute
     * @return ?ReflectionAttribute<object>
     */
    private static function getAttribute(
        ReflectionObject $reflection,
        string $methodName,
        string $attribute,
    ): ?ReflectionAttribute {
        $attributes = $reflection->getMethod($methodName)->getAttributes($attribute);
        if (count($attributes) > 0) {
            return $attributes[0];
        }
        return null;
    }

    public static function getBucketByDisplayName(
        ClientWrapper $clientWrapper,
        string $bucketDisplayName,
        string $stage,
    ): ?array {
        // the client has method getBucketId, but it does not work with display name, and actually it is not
        // useful at all https://keboola.slack.com/archives/CFVRE56UA/p1680696020855349
        $buckets = $clientWrapper->getTableAndFileStorageClient()->listBuckets();
        foreach ($buckets as $bucket) {
            if ($bucket['displayName'] === $bucketDisplayName && $bucket['stage'] === $stage) {
                return $bucket;
            }
        }
        return null;
    }

    private static function ensureEmptyBucket(
        ClientWrapper $clientWrapper,
        string $bucketName,
        string $stage,
        string $backend,
    ): string {
        $bucket = self::getBucketByDisplayName($clientWrapper, $bucketName, $stage);
        if ($bucket !== null) {
            $bucketId = $bucket['id'];
            if ($bucket['backend'] === $backend) {
                $tables = $clientWrapper->getTableAndFileStorageClient()->listTables($bucketId, ['include' => '']);
                foreach ($tables as $table) {
                    $clientWrapper->getTableAndFileStorageClient()->dropTable($table['id']);
                }
                return $bucketId;
            }

            $clientWrapper->getTableAndFileStorageClient()->dropBucket(
                $bucketId,
                ['force' => true, 'async' => true],
            );
        }

        return $clientWrapper->getTableAndFileStorageClient()->createBucket(
            name: $bucketName,
            stage: $stage,
            backend: $backend,
        );
    }

    /**
     * @param ReflectionAttribute<object> $attribute
     */
    private static function getTableCount(ReflectionAttribute $attribute): int
    {
        $arguments = $attribute->getArguments();
        if (count($arguments) > 0) {
            return (int) $arguments[0];
        }
        return 1;
    }

    /**
     * @return array{
     *      emptyOutputBucketId: ?string,
     *      emptyInputBucketId: ?string,
     *      testBucketId: ?string,
     *      firstTableId: ?string,
     *      secondTableId: ?string,
     *      thirdTableId: ?string
     *  }
     */
    public static function satisfyTestNeeds(
        ReflectionObject $reflection,
        ClientWrapper $clientWrapper,
        Temp $temp,
        string $methodName,
        string $dataName,
    ): array {
        $storageBackend = self::getStorageBackendFromAttribute($reflection) ?: 'snowflake';

        $emptyOutputBucket = self::getAttribute($reflection, $methodName, NeedsEmptyOutputBucket::class);
        $emptyInputBucket = self::getAttribute($reflection, $methodName, NeedsEmptyInputBucket::class);

        $testTable = self::getAttribute($reflection, $methodName, NeedsTestTables::class);

        $testResourceName = substr(
            sprintf(
                '%s_%s%s',
                $reflection->getShortName(),
                $methodName,
                $dataName ? '_' . preg_replace('/[^a-zA-Z0-9-_]/', '-', $dataName) : '',
            ),
            0,
            96,
        );

        if ($emptyOutputBucket !== null) {
            $emptyOutputBucketId = self::ensureEmptyBucket(
                $clientWrapper,
                $testResourceName,
                Client::STAGE_OUT,
                $storageBackend,
            );
        }

        if ($emptyInputBucket !== null) {
            $emptyInputBucketId = self::ensureEmptyBucket(
                $clientWrapper,
                $testResourceName,
                Client::STAGE_IN,
                $storageBackend,
            );
        }

        if ($testTable !== null) {
            $testBucketId = self::ensureEmptyBucket(
                $clientWrapper,
                $testResourceName,
                Client::STAGE_IN,
                $storageBackend,
            );

            $csv = new CsvFile($temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'upload.csv');
            $csv->writeRow(['Id', 'Name', 'foo', 'bar']);
            $csv->writeRow(['id1', 'name1', 'foo1', 'bar1']);
            $csv->writeRow(['id2', 'name2', 'foo2', 'bar2']);
            $csv->writeRow(['id3', 'name3', 'foo3', 'bar3']);

            $tableCount = self::getTableCount($testTable);
            $tableIds = [];
            // Create table
            for ($i = 0; $i < $tableCount; $i++) {
                $tableIds[$i] = $clientWrapper->getTableAndFileStorageClient()->createTableAsync(
                    $testBucketId,
                    'test' . ($i + 1),
                    $csv,
                );
            }
        }

        $devBranch = self::getAttribute($reflection, $methodName, NeedsDevBranch::class);
        if ($devBranch) {
            $devBranchId = self::ensureDevBranch($clientWrapper, $testResourceName);
        }

        return [
            'emptyOutputBucketId' => !empty($emptyOutputBucketId) ? (string) $emptyOutputBucketId : null,
            'emptyInputBucketId' => !empty($emptyInputBucketId) ? (string) $emptyInputBucketId : null,
            'testBucketId' => !empty($testBucketId) ? (string) $testBucketId : null,
            'firstTableId' => !empty($tableIds[0]) ? (string) $tableIds[0] : null,
            'secondTableId' => !empty($tableIds[1]) ? (string) $tableIds[1] : null,
            'thirdTableId' => !empty($tableIds[2]) ? (string) $tableIds[2] : null,
            'devBranchName' => !empty($devBranchId) ? $testResourceName : null,
            'devBranchId' => !empty($devBranchId) ? $devBranchId : null,
        ];
    }

    private static function getStorageBackendFromAttribute(ReflectionObject $reflection): ?string
    {
        $attributes = $reflection->getAttributes(NeedsStorageBackend::class);
        if (count($attributes) > 0) {
            /** @var NeedsStorageBackend $needsStorageBackend */
            $needsStorageBackend = $attributes[0]->newInstance();
            return $needsStorageBackend->backend;
        }
        return null;
    }

    private static function ensureDevBranch(
        ClientWrapper $clientWrapper,
        string $branchName,
    ): string {
        $clientWrapper = new ClientWrapper(
            new ClientOptions(
                $clientWrapper->getClientOptionsReadOnly()->getUrl(),
                (string) getenv('STORAGE_API_TOKEN_MASTER'),
            ),
        );

        $branches = new DevBranches($clientWrapper->getBasicClient());
        foreach ($branches->listBranches() as $branch) {
            if ($branch['name'] === $branchName) {
                $branches->deleteBranch($branch['id']);
            }
        }
        return (string) $branches->createBranch($branchName)['id'];
    }
}
