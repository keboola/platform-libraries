<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table\Source;

use Keboola\FileStorage\Abs\ClientFactory;
use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\InitSynapseStorageClientTrait;
use Keboola\OutputMapping\Writer\Table\Source\AbsWorkspaceItemSourceFactory;
use Keboola\StorageApi\ABSUploader;
use Keboola\StorageApi\Workspaces;
use Keboola\Temp\Temp;
use MicrosoftAzure\Storage\Blob\Models\CreateBlockBlobOptions;

class AbsWorkspaceItemSourceFactoryTest extends AbstractTestCase
{
    use InitSynapseStorageClientTrait;

    protected function initClient(?string $branchId = ''): void
    {
        $this->clientWrapper = $this->getSynapseClientWrapper();
    }

    public function testCreateSource(): void
    {
        $workspaces = new Workspaces($this->clientWrapper->getBranchClient());
        $workspace = $workspaces->createWorkspace(['backend' => 'abs'], true);
        $this->workspaceId = (string) $workspace['id'];
        $this->workspace = $workspace;

        $providerMock = $this->createMock(ProviderInterface::class);
        $providerMock->expects(self::exactly(2))
            ->method('getWorkspaceId')
            ->willReturn($this->workspaceId)
        ;
        ;

        $providerMock->expects(self::exactly(2))
            ->method('getCredentials')
            ->willReturn($this->workspace['connection'])
        ;

        $blobClient = ClientFactory::createClientFromConnectionString(
            $this->workspace['connection']['connectionString'],
        );

        $temp = new Temp();
        $file = $temp->createFile('myName.csv');

        $options = new CreateBlockBlobOptions();

        $uploader = new ABSUploader($blobClient);
        $uploader->uploadFile(
            $this->workspace['connection']['container'],
            'my-file/myName.csv',
            $file->getPathname(),
            $options,
            false,
        );

        $uploader->uploadFile(
            $this->workspace['connection']['container'],
            'my-file/myAnotherName.csv/part1',
            $file->getPathname(),
            $options,
            false,
        );

        $factory = new AbsWorkspaceItemSourceFactory($providerMock);

        $workspaceItemSource = $factory->createSource('my-file', 'myName.csv');
        self::assertSame($this->workspaceId, $workspaceItemSource->getWorkspaceId());
        self::assertSame('myName.csv', $workspaceItemSource->getName());
        self::assertSame('my-file/myName.csv', $workspaceItemSource->getDataObject());
        self::assertFalse($workspaceItemSource->isSliced());

        $workspaceItemSource = $factory->createSource('my-file', 'myAnotherName.csv');
        self::assertSame($this->workspaceId, $workspaceItemSource->getWorkspaceId());
        self::assertSame('myAnotherName.csv', $workspaceItemSource->getName());
        self::assertSame('my-file/myAnotherName.csv/', $workspaceItemSource->getDataObject());
        self::assertTrue($workspaceItemSource->isSliced());
    }

    public function testCreateSourceFromNonExistingContainerThrowsException(): void
    {
        $workspaces = new Workspaces($this->clientWrapper->getBranchClient());
        $workspace = $workspaces->createWorkspace(['backend' => 'abs'], true);
        $this->workspaceId = (string) $workspace['id'];
        $this->workspace = $workspace;

        $providerMock = $this->createMock(ProviderInterface::class);
        $providerMock->expects(self::once())
            ->method('getCredentials')
            ->willReturn(array_merge(
                $this->workspace['connection'],
                ['container' => 'dummy'],
            ))
        ;

        $factory = new AbsWorkspaceItemSourceFactory($providerMock);

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Failed to list blobs');
        $this->expectExceptionMessage('AuthenticationFailed');

        $factory->createSource('my-file', 'myName.csv');
    }
}
