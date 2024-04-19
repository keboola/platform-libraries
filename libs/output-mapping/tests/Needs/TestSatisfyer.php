<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Needs;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApiBranch\ClientWrapper;
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

    public static function getBucketIdByDisplayName(
        ClientWrapper $clientWrapper,
        string $bucketDisplayName,
        string $stage,
    ): ?string {
        // the client has method getBucketId, but it does not work with display name, and actually it is not
        // useful at all https://keboola.slack.com/archives/CFVRE56UA/p1680696020855349
        $buckets = $clientWrapper->getTableAndFileStorageClient()->listBuckets();
        foreach ($buckets as $bucket) {
            if ($bucket['displayName'] === $bucketDisplayName && $bucket['stage'] === $stage) {
                return $bucket['id'];
            }
        }
        return null;
    }

    private static function ensureEmptyBucket(
        ClientWrapper $clientWrapper,
        string $bucketName,
        string $stage,
        string $backend = 'snowflake',
    ): string {
        $bucketId = self::getBucketIdByDisplayName($clientWrapper, $bucketName, $stage);
        if ($bucketId !== null) {
            $tables = $clientWrapper->getTableAndFileStorageClient()->listTables($bucketId, ['include' => '']);
            foreach ($tables as $table) {
                $clientWrapper->getTableAndFileStorageClient()->dropTable($table['id']);
            }
            return $bucketId;
        }
        return $clientWrapper->getTableAndFileStorageClient()->createBucket(
            name: $bucketName,
            stage: $stage,
            backend: $backend,
        );
    }

    private static function ensureRemoveBucket(
        ClientWrapper $clientWrapper,
        string $bucketId,
    ): void {
        try {
            $clientWrapper->getTableAndFileStorageClient()->dropBucket($bucketId);
        } catch (ClientException $e) {
            if ($e->getCode() !== 404) {
                throw $e;
            }
        }
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
     *      emptyRedshiftOutputBucketId: ?string,
     *      emptyRedshiftInputBucketId: ?string,
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
    ): array {
        $emptyOutputBucket = self::getAttribute($reflection, $methodName, NeedsEmptyOutputBucket::class);
        $emptyInputBucket = self::getAttribute($reflection, $methodName, NeedsEmptyInputBucket::class);
        $emptyRedshiftOutputBucket = self::getAttribute(
            $reflection,
            $methodName,
            NeedsEmptyRedshiftOutputBucket::class,
        );
        $emptyRedshiftInputBucket = self::getAttribute(
            $reflection,
            $methodName,
            NeedsEmptyRedshiftInputBucket::class,
        );

        $removeBucket = self::getAttribute($reflection, $methodName, NeedsRemoveBucket::class);

        $testTable = self::getAttribute($reflection, $methodName, NeedsTestTables::class);

        if ($removeBucket !== null) {
            self::ensureRemoveBucket($clientWrapper, $removeBucket->getArguments()[0]);
        }

        if ($emptyOutputBucket !== null) {
            $emptyOutputBucketId = self::ensureEmptyBucket($clientWrapper, $methodName . 'Empty', Client::STAGE_OUT);
        }

        if ($emptyInputBucket !== null) {
            $emptyInputBucketId = self::ensureEmptyBucket($clientWrapper, $methodName . 'Empty', Client::STAGE_IN);
        }

        if ($emptyRedshiftOutputBucket !== null) {
            $emptyRedshiftOutputBucketId = self::ensureEmptyBucket(
                $clientWrapper,
                $methodName . 'Empty',
                Client::STAGE_OUT,
                'redshift',
            );
        }

        if ($emptyRedshiftInputBucket !== null) {
            $emptyRedshiftInputBucketId = self::ensureEmptyBucket(
                $clientWrapper,
                $methodName . 'Empty',
                Client::STAGE_IN,
                'redshift',
            );
        }

        if ($testTable !== null) {
            $testBucketId = self::ensureEmptyBucket($clientWrapper, $methodName . 'Test', Client::STAGE_IN);

            $csv = new CsvFile($temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'upload.csv');
            $csv->writeRow(['Id', 'Name', 'foo', 'bar']);
            $csv->writeRow(['id1', 'name1', 'foo1', 'bar1']);
            $csv->writeRow(['id2', 'name2', 'foo2', 'bar2']);
            $csv->writeRow(['id3', 'name3', 'foo3', 'bar3']);

            $tableCount = self::getTableCount($testTable);
            $tableIds = [];
            // Create table
            $propNames = ['firstTableId', 'secondTableId', 'thirdTableId'];
            for ($i = 0; $i < max($tableCount, count($propNames)); $i++) {
                $tableIds[$i] = $clientWrapper->getTableAndFileStorageClient()->createTableAsync(
                    $testBucketId,
                    'test' . ($i + 1),
                    $csv,
                );
            }
        }

        return [
            'emptyOutputBucketId' => !empty($emptyOutputBucketId) ? (string) $emptyOutputBucketId : null,
            'emptyInputBucketId' => !empty($emptyInputBucketId) ? (string) $emptyInputBucketId : null,
            'emptyRedshiftOutputBucketId' => !empty($emptyRedshiftOutputBucketId) ?
                (string) $emptyRedshiftOutputBucketId :
                null,
            'emptyRedshiftInputBucketId' => !empty($emptyRedshiftInputBucketId) ?
                (string) $emptyRedshiftInputBucketId :
                null,
            'testBucketId' => !empty($testBucketId) ? (string) $testBucketId : null,
            'firstTableId' => !empty($tableIds[0]) ? (string) $tableIds[0] : null,
            'secondTableId' => !empty($tableIds[1]) ? (string) $tableIds[1] : null,
            'thirdTableId' => !empty($tableIds[2]) ? (string) $tableIds[2] : null,
        ];
    }
}
