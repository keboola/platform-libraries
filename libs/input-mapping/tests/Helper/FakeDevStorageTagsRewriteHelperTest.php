<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Helper;

use Keboola\InputMapping\File\Options\InputFileOptions;
use Keboola\InputMapping\Helper\FakeDevStorageTagsRewriteHelper;
use Keboola\InputMapping\Tests\AbstractTestCase;
use Keboola\InputMapping\Tests\Needs\NeedsDevBranch;
use Keboola\StorageApi\Options\FileUploadOptions;
use Monolog\Handler\TestHandler;
use Monolog\Logger;

class FakeDevStorageTagsRewriteHelperTest extends AbstractTestCase
{
    private string $testFileTag;

    public function setUp(): void
    {
        parent::setUp();

        $this->testFileTag = $this->getFileTag();
        $this->clearFileUploads([$this->testFileTag]);
    }

    public function testNoBranch(): void
    {
        $configuration = ['tags' => [$this->testFileTag]];
        $clientWrapper = $this->initClient();
        $testHandler = new TestHandler();
        $testLogger = new Logger('testLogger', [$testHandler]);
        $processedConfiguration = (new FakeDevStorageTagsRewriteHelper())->rewriteFileTags(
            new InputFileOptions(
                $configuration,
                $clientWrapper->isDevelopmentBranch(),
                (string) $clientWrapper->getClientOptionsReadOnly()->getRunId(),
            ),
            $clientWrapper,
            $testLogger,
        )->getDefinition();
        $expectedConfiguration = $configuration;
        $expectedConfiguration['overwrite'] = true;

        self::assertSame($expectedConfiguration, $processedConfiguration);
    }

    #[NeedsDevBranch]
    public function testBranchRewriteFilesExists(): void
    {
        $branchTag = $this->getFakeBranchFileTag();
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload', 'test');
        $clientWrapper = $this->initClient($this->devBranchId);
        $clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$branchTag]),
        );
        sleep(2);

        $configuration = ['tags' => [$this->testFileTag]];

        $configuration = new InputFileOptions(
            $configuration,
            $clientWrapper->isDevelopmentBranch(),
            (string) $clientWrapper->getClientOptionsReadOnly()->getRunId(),
        );

        $testHandler = new TestHandler();
        $testLogger = new Logger('testLogger', [$testHandler]);
        $expectedConfiguration = (new FakeDevStorageTagsRewriteHelper())->rewriteFileTags(
            $configuration,
            $clientWrapper,
            $testLogger,
        )->getDefinition();

        self::assertTrue($testHandler->hasInfoThatContains(
            sprintf('Using dev tags "%s" instead of "' . $this->testFileTag . '".', $branchTag),
        ));

        self::assertEquals([$branchTag], $expectedConfiguration['tags']);
    }

    #[NeedsDevBranch]
    public function testBranchRewriteSourceTagsFilesExists(): void
    {
        $branchTag = $this->getFakeBranchFileTag();
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload', 'test');

        $clientWrapper = $this->initClient($this->devBranchId);
        $clientWrapper
            ->getTableAndFileStorageClient()
            ->uploadFile($root . '/upload', (new FileUploadOptions())->setTags([$branchTag]));
        sleep(2);

        $configuration = [
            'source' => [
                'tags' => [
                    [
                        'name' => $this->testFileTag,
                        'match' => 'include',
                    ],
                ],
            ],
        ];

        $testHandler = new TestHandler();
        $testLogger = new Logger('testLogger', [$testHandler]);
        $expectedConfiguration = (new FakeDevStorageTagsRewriteHelper())->rewriteFileTags(
            new InputFileOptions(
                $configuration,
                $clientWrapper->isDevelopmentBranch(),
                (string) $clientWrapper->getClientOptionsReadOnly()->getRunId(),
            ),
            $clientWrapper,
            $testLogger,
        )->getDefinition();
        self::assertTrue(
            $testHandler->hasInfoThatContains(
                sprintf(
                    'Using dev source tags "%s" instead of "' . $this->testFileTag . '".',
                    $branchTag,
                ),
            ),
        );

        self::assertEquals(
            [[
                'name' => $branchTag,
                'match' => 'include',
            ]],
            $expectedConfiguration['source']['tags'],
        );
    }

    #[NeedsDevBranch]
    public function testBranchRewriteNoFiles(): void
    {
        $branchTag = $this->getFakeBranchFileTag();
        $configuration = ['tags' => [$this->testFileTag]];
        $testHandler = new TestHandler();
        $testLogger = new Logger('testLogger', [$testHandler]);
        sleep(2); // wait for storage files cleanup to take effect
        $clientWrapper = $this->initClient($this->devBranchId);
        $processedConfiguration = (new FakeDevStorageTagsRewriteHelper())->rewriteFileTags(
            new InputFileOptions(
                $configuration,
                $clientWrapper->isDevelopmentBranch(),
                (string) $clientWrapper->getClientOptionsReadOnly()->getRunId(),
            ),
            $clientWrapper,
            $testLogger,
        )->getDefinition();

        self::assertFalse($testHandler->hasInfoThatContains(
            sprintf('Using dev tags "%s" instead of "' . $this->testFileTag . '".', $branchTag),
        ));
        $expectedConfiguration = $configuration;
        $expectedConfiguration['overwrite'] = true;

        self::assertEquals($expectedConfiguration, $processedConfiguration);
    }

    #[NeedsDevBranch]
    public function testBranchRewriteSourceTagsNoFiles(): void
    {
        $branchTag = $this->getFakeBranchFileTag();
        $configuration = [
            'source' => [
                'tags' => [
                    [
                        'name' => $this->testFileTag,
                        'match' => 'include',
                    ],
                ],
            ],
        ];

        $testHandler = new TestHandler();
        $testLogger = new Logger('testLogger', [$testHandler]);
        $clientWrapper = $this->initClient($this->devBranchId);
        $processedConfiguration = (new FakeDevStorageTagsRewriteHelper())->rewriteFileTags(
            new InputFileOptions(
                $configuration,
                $clientWrapper->isDevelopmentBranch(),
                (string) $clientWrapper->getClientOptionsReadOnly()->getRunId(),
            ),
            $clientWrapper,
            $testLogger,
        )->getDefinition();

        self::assertFalse(
            $testHandler->hasInfoThatContains(
                sprintf('Using dev tags "%s" instead of "' . $this->testFileTag . '".', $branchTag),
            ),
        );
        $expectedConfiguration = $configuration;
        $expectedConfiguration['overwrite'] = true;

        self::assertEquals($expectedConfiguration, $processedConfiguration);
    }

    #[NeedsDevBranch]
    public function testBranchRewriteExcludedProcessedSourceTagFilesExist(): void
    {
        $branchTag = $this->getFakeBranchFileTag();
        $root = $this->temp->getTmpFolder();
        $branchProcessedTag = sprintf('%s-processed', $this->devBranchId);
        file_put_contents($root . '/upload', 'test');
        $clientWrapper = $this->initClient($this->devBranchId);
        $clientWrapper
            ->getTableAndFileStorageClient()
            ->uploadFile(
                $root . '/upload',
                (new FileUploadOptions())->setTags([$branchTag, $branchProcessedTag]),
            );
        sleep(2);
        $configuration = [
            'source' => [
                'tags' => [
                    [
                        'name' => $branchTag,
                        'match' => 'include',
                    ],
                    [
                        'name' => 'processed',
                        'match' => 'exclude',
                    ],
                ],
            ],
            'processed_tags' => ['processed'],
        ];
        $testHandler = new TestHandler();
        $testLogger = new Logger('testLogger', [$testHandler]);
        $expectedConfiguration = (new FakeDevStorageTagsRewriteHelper())->rewriteFileTags(
            new InputFileOptions(
                $configuration,
                $clientWrapper->isDevelopmentBranch(),
                (string) $clientWrapper->getClientOptionsReadOnly()->getRunId(),
            ),
            $clientWrapper,
            $testLogger,
        )->getDefinition();
        // it should rewrite the processed exclude tag
        self::assertContains(
            [
                'name' => $branchProcessedTag,
                'match' => 'exclude',
            ],
            $expectedConfiguration['source']['tags'],
        );
    }

    #[NeedsDevBranch]
    public function testBranchRewriteExcludedProcessedSourceTagBranchFileDoesNotExist(): void
    {
        $branchTag = $this->getFakeBranchFileTag();
        $root = $this->temp->getTmpFolder();
        $branchProcessedTag = sprintf('%s-processed', $this->devBranchId);
        file_put_contents($root . '/upload', 'test');
        $clientWrapper = $this->initClient($this->devBranchId);
        $clientWrapper
            ->getTableAndFileStorageClient()
            ->uploadFile(
                $root . '/upload',
                (new FileUploadOptions())->setTags([$this->testFileTag, 'processed']),
            );
        sleep(2);
        $configuration = [
            'source' => [
                'tags' => [
                    [
                        'name' => $this->testFileTag,
                        'match' => 'include',
                    ],
                    [
                        'name' => 'processed',
                        'match' => 'exclude',
                    ],
                ],
            ],
            'processed_tags' => ['processed'],
        ];
        $testHandler = new TestHandler();
        $testLogger = new Logger('testLogger', [$testHandler]);
        $expectedConfiguration = (new FakeDevStorageTagsRewriteHelper())->rewriteFileTags(
            new InputFileOptions(
                $configuration,
                $clientWrapper->isDevelopmentBranch(),
                (string) $clientWrapper->getClientOptionsReadOnly()->getRunId(),
            ),
            $clientWrapper,
            $testLogger,
        )->getDefinition();
        // it should NOT rewrite the include tag because there is no branch file that exists
        // but it SHOULD rewrite the processed tag for this branch
        self::assertEquals(
            [[
                'name' => $this->testFileTag,
                'match' => 'include',
            ], [
                'name' => $branchProcessedTag,
                'match' => 'exclude',
            ]],
            $expectedConfiguration['source']['tags'],
        );
    }

    private function getFakeBranchFileTag(): string
    {
        return sprintf('%s-%s', $this->devBranchId, $this->testFileTag);
    }
}
