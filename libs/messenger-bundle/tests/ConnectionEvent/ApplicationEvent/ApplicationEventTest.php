<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\Tests\ConnectionEvent\ApplicationEvent;

use InvalidArgumentException;
use Keboola\MessengerBundle\ConnectionEvent\ApplicationEvent\GenericApplicationEvent;
use PHPUnit\Framework\TestCase;

class ApplicationEventTest extends TestCase
{
    public function testFromArrayWithMinimalData(): void
    {
        $event = GenericApplicationEvent::fromArray([
            'name' => 'ext.keboola.keboola-buffer.',
            'uuid' => '01958fcb-6cf6-778b-ac16-fb7cd4f9ab3d',
            'type' => 'info',
        ]);

        self::assertSame('ext.keboola.keboola-buffer.', $event->name);
        self::assertSame('01958fcb-6cf6-778b-ac16-fb7cd4f9ab3d', $event->uuid);
        self::assertSame('info', $event->type);
        self::assertNull($event->idAdmin);
        self::assertNull($event->idProject);
        self::assertNull($event->idAccessToken);
        self::assertNull($event->accessTokenName);
        self::assertNull($event->description);
        self::assertNull($event->message);
        self::assertNull($event->component);
        self::assertNull($event->runId);
        self::assertNull($event->idBucket);
        self::assertNull($event->tableName);
        self::assertNull($event->idAccount);
        self::assertNull($event->idExport);
        self::assertNull($event->configurationId);
        self::assertNull($event->idWorkspace);
        self::assertNull($event->fileIds);
        self::assertNull($event->objectType);
        self::assertNull($event->objectName);
        self::assertNull($event->objectId);
        self::assertNull($event->context);
        self::assertNull($event->params);
        self::assertNull($event->results);
        self::assertNull($event->performance);
        self::assertNull($event->idBranch);
        self::assertNull($event->sendNotificationEmail);
    }

    public function testFromArrayWithAllData(): void
    {
        $event = GenericApplicationEvent::fromArray([
            'name' => 'ext.keboola.keboola-buffer.',
            'objectId' => 'object-id',
            'idProject' => 18,
            'idAdmin' => 19,
            'idAccount' => 20,
            'idExport' => 21,
            'idAccessToken' => 130,
            'accessTokenName' => '[_internal] Buffer Export acceptance-test',
            'idBucket' => 22,
            'idWorkspace' => 23,
            'tableName' => 'table',
            'objectType' => 'object-type',
            'objectName' => 'object-name',
            'params' => [
                'task' => 'file-import',
            ],
            'performance' => [
                'foo' => 'bar',
            ],
            'uuid' => '01958fcb-6cf6-778b-ac16-fb7cd4f9ab3d',
            'type' => 'info',
            'description' => 'event description',
            'context' => [
                'remoteAddr' => '10.11.2.62',
                'httpReferer' => null,
                'httpUserAgent' => 'keboola-buffer-worker',
                'apiVersion' => 'v2',
            ],
            'results' => [
                'exportId' => 'acceptance-test-export',
                'projectId' => 18,
                'statistics' =>  [
                    'bodySize' => 0,
                    'fileGZipSize' => 0,
                ],
            ],
            'component' => 'keboola.keboola-buffer',
            'message' => 'File import done.',
            'runId' => '55',
            'configurationId' => '66',
            'fileIds' => [11, '12'],
            'idBranch' => 7,
            'sendNotificationEmail' => true,
        ]);

        self::assertSame('ext.keboola.keboola-buffer.', $event->name);
        self::assertSame('object-id', $event->objectId);
        self::assertSame(18, $event->idProject);
        self::assertSame(19, $event->idAdmin);
        self::assertSame(20, $event->idAccount);
        self::assertSame(21, $event->idExport);
        self::assertSame(130, $event->idAccessToken);
        self::assertSame('[_internal] Buffer Export acceptance-test', $event->accessTokenName);
        self::assertSame(22, $event->idBucket);
        self::assertSame(23, $event->idWorkspace);
        self::assertSame('table', $event->tableName);
        self::assertSame('object-type', $event->objectType);
        self::assertSame('object-name', $event->objectName);
        self::assertSame(['task' => 'file-import'], $event->params);
        self::assertSame(['foo' => 'bar'], $event->performance);
        self::assertSame('01958fcb-6cf6-778b-ac16-fb7cd4f9ab3d', $event->uuid);
        self::assertSame('info', $event->type);
        self::assertSame('event description', $event->description);
        self::assertSame([
            'remoteAddr' => '10.11.2.62',
            'httpReferer' => null,
            'httpUserAgent' => 'keboola-buffer-worker',
            'apiVersion' => 'v2',
        ], $event->context);
        self::assertSame([
            'exportId' => 'acceptance-test-export',
            'projectId' => 18,
            'statistics' =>  [
                'bodySize' => 0,
                'fileGZipSize' => 0,
            ],
        ], $event->results);
        self::assertSame('keboola.keboola-buffer', $event->component);
        self::assertSame('File import done.', $event->message);
        self::assertSame('55', $event->runId);
        self::assertSame('66', $event->configurationId);
        self::assertSame([11, '12'], $event->fileIds);
        self::assertSame(7, $event->idBranch);
        self::assertTrue($event->sendNotificationEmail);
    }

    public static function provideInvalidData(): iterable
    {
        yield 'missing name' => [
            'data' => [
                'uuid' => '01958fcb-6cf6-778b-ac16-fb7cd4f9ab3d',
                'type' => 'info',
            ],
            'error' => 'Event is missing property "name"',
        ];

        yield 'missing uuid' => [
            'data' => [
                'name' => 'ext.keboola.keboola-buffer.',
                'type' => 'info',
            ],
            'error' => 'Event is missing property "uuid"',
        ];

        yield 'missing type' => [
            'data' => [
                'name' => 'ext.keboola.keboola-buffer.',
                'uuid' => '01958fcb-6cf6-778b-ac16-fb7cd4f9ab3d',
            ],
            'error' => 'Event is missing property "type"',
        ];
    }

    /** @dataProvider provideInvalidData */
    public function testFromArrayWithMissingRequiredProperty(array $data, string $expectedError): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($expectedError);

        GenericApplicationEvent::fromArray($data);
    }

    public function testToArrayWithMinimalData(): void
    {
        $event = new GenericApplicationEvent(
            name: 'ext.keboola.keboola-buffer.',
            uuid: '01958fcb-6cf6-778b-ac16-fb7cd4f9ab3d',
            type: 'info',
        );

        self::assertSame([
            'name' => 'ext.keboola.keboola-buffer.',
            'uuid' => '01958fcb-6cf6-778b-ac16-fb7cd4f9ab3d',
            'type' => 'info',
            'idAdmin' => null,
            'idProject' => null,
            'idAccessToken' => null,
            'accessTokenName' => null,
            'description' => null,
            'message' => null,
            'component' => null,
            'runId' => null,
            'idBucket' => null,
            'tableName' => null,
            'idAccount' => null,
            'idExport' => null,
            'configurationId' => null,
            'idWorkspace' => null,
            'fileIds' => null,
            'objectType' => null,
            'objectName' => null,
            'objectId' => null,
            'context' => null,
            'params' => null,
            'results' => null,
            'performance' => null,
            'idBranch' => null,
            'sendNotificationEmail' => null,
        ], $event->toArray());
    }

    public function testToArrayWithAllData(): void
    {
        $event = new GenericApplicationEvent(
            name: 'ext.keboola.keboola-buffer.',
            uuid: '01958fcb-6cf6-778b-ac16-fb7cd4f9ab3d',
            type: 'info',
            idAdmin: 19,
            idProject: 18,
            idAccessToken: 130,
            accessTokenName: '[_internal] Buffer Export acceptance-test',
            description: 'event description',
            message: 'File import done.',
            component: 'keboola.keboola-buffer',
            runId: '55',
            idBucket: 22,
            tableName: 'table',
            idAccount: 20,
            idExport: 21,
            configurationId: '66',
            idWorkspace: 23,
            fileIds: [11, '12'],
            objectType: 'object-type',
            objectName: 'object-name',
            objectId: 'object-id',
            context: [
                'remoteAddr' => '10.11.2.62',
                'httpReferer' => null,
                'httpUserAgent' => 'keboola-buffer-worker',
                'apiVersion' => 'v2',
            ],
            params: [
                'task' => 'file-import',
            ],
            results: [
                'exportId' => 'acceptance-test-export',
                'projectId' => 18,
                'statistics' =>  [
                    'bodySize' => 0,
                    'fileGZipSize' => 0,
                ],
            ],
            performance: [
                'foo' => 'bar',
            ],
            idBranch: 7,
            sendNotificationEmail: true,
        );

        self::assertSame([
            'name' => 'ext.keboola.keboola-buffer.',
            'uuid' => '01958fcb-6cf6-778b-ac16-fb7cd4f9ab3d',
            'type' => 'info',
            'idAdmin' => 19,
            'idProject' => 18,
            'idAccessToken' => 130,
            'accessTokenName' => '[_internal] Buffer Export acceptance-test',
            'description' => 'event description',
            'message' => 'File import done.',
            'component' => 'keboola.keboola-buffer',
            'runId' => '55',
            'idBucket' => 22,
            'tableName' => 'table',
            'idAccount' => 20,
            'idExport' => 21,
            'configurationId' => '66',
            'idWorkspace' => 23,
            'fileIds' => [11, '12'],
            'objectType' => 'object-type',
            'objectName' => 'object-name',
            'objectId' => 'object-id',
            'context' => [
                'remoteAddr' => '10.11.2.62',
                'httpReferer' => null,
                'httpUserAgent' => 'keboola-buffer-worker',
                'apiVersion' => 'v2',
            ],
            'params' => [
                'task' => 'file-import',
            ],
            'results' => [
                'exportId' => 'acceptance-test-export',
                'projectId' => 18,
                'statistics' =>  [
                    'bodySize' => 0,
                    'fileGZipSize' => 0,
                ],
            ],
            'performance' => [
                'foo' => 'bar',
            ],
            'idBranch' => 7,
            'sendNotificationEmail' => true,
        ], $event->toArray());
    }

    public function testGetters(): void
    {
        $event = GenericApplicationEvent::fromArray([
            'name' => 'ext.keboola.keboola-buffer.',
            'uuid' => '01958fcb-6cf6-778b-ac16-fb7cd4f9ab3d',
            'type' => 'info',
        ]);

        self::assertSame('ext.keboola.keboola-buffer.', $event->getEventName());
        self::assertSame('01958fcb-6cf6-778b-ac16-fb7cd4f9ab3d', $event->getId());
    }
}
