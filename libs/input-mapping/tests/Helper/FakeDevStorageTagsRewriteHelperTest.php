<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Helper;

use Keboola\InputMapping\File\Options\InputFileOptions;
use Keboola\InputMapping\Helper\FakeDevStorageTagsRewriteHelper;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;

class FakeDevStorageTagsRewriteHelperTest extends TestCase
{
    private const TEST_REWRITE_BASE_TAG = 'im-files-test';

    private static string $branchId;
    private static string $branchTag;
    protected string $tmpDir;

    public function setUp(): void
    {
        parent::setUp();

        // Create folders
        $temp = new Temp('docker');
        $this->tmpDir = $temp->getTmpFolder();
        sleep(2);
        $clientWrapper = self::getClientWrapper(null);
        $files = $clientWrapper->getTableAndFileStorageClient()->listFiles(
            (new ListFilesOptions())->setTags([self::TEST_REWRITE_BASE_TAG, self::$branchTag])
        );
        foreach ($files as $file) {
            $clientWrapper->getTableAndFileStorageClient()->deleteFile($file['id']);
        }
    }

    public static function setUpBeforeClass(): void
    {
        parent::setUpBeforeClass();
        $branchesApi = new DevBranches(self::getClientWrapper(null)->getBasicClient());
        self::$branchId = (string) $branchesApi->createBranch(uniqid('TagsRewriteHelperTest'))['id'];
        self::$branchTag = sprintf('%s-' . self::TEST_REWRITE_BASE_TAG, self::$branchId);
    }

    public static function tearDownAfterClass(): void
    {
        $branchesApi = new DevBranches(self::getClientWrapper(null)->getBasicClient());
        $branchesApi->deleteBranch((int) self::$branchId);
        parent::tearDownAfterClass();
    }

    private static function getClientWrapper(?string $branchId): ClientWrapper
    {
        return new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('STORAGE_API_TOKEN_MASTER'),
                $branchId
            ),
        );
    }

    public function testNoBranch(): void
    {
        $configuration = ['tags' => [self::TEST_REWRITE_BASE_TAG]];
        $clientWrapper = self::getClientWrapper(null);
        $testLogger = new TestLogger();
        $processedConfiguration = (new FakeDevStorageTagsRewriteHelper())->rewriteFileTags(
            new InputFileOptions(
                $configuration,
                $clientWrapper->hasBranch(),
                (string) $clientWrapper->getClientOptionsReadOnly()->getRunId()
            ),
            $clientWrapper,
            $testLogger,
        )->getDefinition();
        $expectedConfiguration = $configuration;
        $expectedConfiguration['overwrite'] = true;

        self::assertSame($expectedConfiguration, $processedConfiguration);
    }

    public function testBranchRewriteFilesExists(): void
    {
        $root = $this->tmpDir;
        file_put_contents($root . '/upload', 'test');
        $clientWrapper = self::getClientWrapper(self::$branchId);
        $clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([self::$branchTag])
        );
        sleep(2);

        $configuration = ['tags' => ['im-files-test']];

        $configuration = new InputFileOptions(
            $configuration,
            $clientWrapper->hasBranch(),
            (string) $clientWrapper->getClientOptionsReadOnly()->getRunId()
        );

        $testLogger = new TestLogger();
        $expectedConfiguration = (new FakeDevStorageTagsRewriteHelper())->rewriteFileTags(
            $configuration,
            $clientWrapper,
            $testLogger
        )->getDefinition();

        self::assertTrue($testLogger->hasInfoThatContains(
            sprintf('Using dev tags "%s" instead of "' . self::TEST_REWRITE_BASE_TAG . '".', self::$branchTag)
        ));

        self::assertEquals([self::$branchTag], $expectedConfiguration['tags']);
    }

    public function testBranchRewriteSourceTagsFilesExists(): void
    {
        $root = $this->tmpDir;
        file_put_contents($root . '/upload', 'test');

        $clientWrapper = self::getClientWrapper(self::$branchId);
        $clientWrapper
            ->getTableAndFileStorageClient()
            ->uploadFile($root . '/upload', (new FileUploadOptions())->setTags([self::$branchTag]));
        sleep(2);

        $configuration = [
            'source' => [
                'tags' => [
                    [
                        'name' => self::TEST_REWRITE_BASE_TAG,
                        'match' => 'include',
                    ],
                ],
            ],
        ];

        $testLogger = new TestLogger();
        $expectedConfiguration = (new FakeDevStorageTagsRewriteHelper())->rewriteFileTags(
            new InputFileOptions(
                $configuration,
                $clientWrapper->hasBranch(),
                (string) $clientWrapper->getClientOptionsReadOnly()->getRunId()
            ),
            $clientWrapper,
            $testLogger
        )->getDefinition();
        self::assertTrue(
            $testLogger->hasInfoThatContains(
                sprintf(
                    'Using dev source tags "%s" instead of "' . self::TEST_REWRITE_BASE_TAG . '".',
                    self::$branchTag
                )
            )
        );

        self::assertEquals(
            [[
                'name' => self::$branchTag,
                'match' => 'include',
            ]],
            $expectedConfiguration['source']['tags']
        );
    }

    public function testBranchRewriteNoFiles(): void
    {
        $configuration = ['tags' => [self::TEST_REWRITE_BASE_TAG]];
        $testLogger = new TestLogger();
        sleep(2); // wait for storage files cleanup to take effect
        $clientWrapper = self::getClientWrapper(self::$branchId);
        $processedConfiguration = (new FakeDevStorageTagsRewriteHelper())->rewriteFileTags(
            new InputFileOptions(
                $configuration,
                $clientWrapper->hasBranch(),
                (string) $clientWrapper->getClientOptionsReadOnly()->getRunId()
            ),
            $clientWrapper,
            $testLogger,
        )->getDefinition();

        self::assertFalse($testLogger->hasInfoThatContains(
            sprintf('Using dev tags "%s" instead of "' . self::TEST_REWRITE_BASE_TAG . '".', self::$branchTag)
        ));
        $expectedConfiguration = $configuration;
        $expectedConfiguration['overwrite'] = true;

        self::assertEquals($expectedConfiguration, $processedConfiguration);
    }

    public function testBranchRewriteSourceTagsNoFiles(): void
    {
        $configuration = [
            'source' => [
                'tags' => [
                    [
                        'name' => self::TEST_REWRITE_BASE_TAG,
                        'match' => 'include',
                    ],
                ],
            ],
        ];

        $testLogger = new TestLogger();
        $clientWrapper = self::getClientWrapper(self::$branchId);
        $processedConfiguration = (new FakeDevStorageTagsRewriteHelper())->rewriteFileTags(
            new InputFileOptions(
                $configuration,
                $clientWrapper->hasBranch(),
                (string) $clientWrapper->getClientOptionsReadOnly()->getRunId()
            ),
            $clientWrapper,
            $testLogger
        )->getDefinition();

        self::assertFalse(
            $testLogger->hasInfoThatContains(
                sprintf('Using dev tags "%s" instead of "' . self::TEST_REWRITE_BASE_TAG . '".', self::$branchTag)
            )
        );
        $expectedConfiguration = $configuration;
        $expectedConfiguration['overwrite'] = true;

        self::assertEquals($expectedConfiguration, $processedConfiguration);
    }

    public function testBranchRewriteExcludedProcessedSourceTagFilesExist(): void
    {
        $branchProcessedTag = sprintf('%s-processed', self::$branchId);
        file_put_contents($this->tmpDir . '/upload', 'test');
        $clientWrapper = self::getClientWrapper(self::$branchId);
        $clientWrapper
            ->getTableAndFileStorageClient()
            ->uploadFile(
                $this->tmpDir . '/upload',
                (new FileUploadOptions())->setTags([self::$branchTag, $branchProcessedTag])
            );
        sleep(2);
        $configuration = [
            'source' => [
                'tags' => [
                    [
                        'name' => self::$branchTag,
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
        $testLogger = new TestLogger();
        $expectedConfiguration = (new FakeDevStorageTagsRewriteHelper())->rewriteFileTags(
            new InputFileOptions(
                $configuration,
                $clientWrapper->hasBranch(),
                (string) $clientWrapper->getClientOptionsReadOnly()->getRunId()
            ),
            $clientWrapper,
            $testLogger
        )->getDefinition();
        // it should rewrite the processed exclude tag
        self::assertContains(
            [
                'name' => $branchProcessedTag,
                'match' => 'exclude',
            ],
            $expectedConfiguration['source']['tags']
        );
    }

    public function testBranchRewriteExcludedProcessedSourceTagBranchFileDoesNotExist(): void
    {
        $branchProcessedTag = sprintf('%s-processed', self::$branchId);
        file_put_contents($this->tmpDir . '/upload', 'test');
        $clientWrapper = self::getClientWrapper(self::$branchId);
        $clientWrapper
            ->getTableAndFileStorageClient()
            ->uploadFile(
                $this->tmpDir . '/upload',
                (new FileUploadOptions())->setTags([self::TEST_REWRITE_BASE_TAG, 'processed'])
            );
        sleep(2);
        $configuration = [
            'source' => [
                'tags' => [
                    [
                        'name' => self::TEST_REWRITE_BASE_TAG,
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
        $testLogger = new TestLogger();
        $expectedConfiguration = (new FakeDevStorageTagsRewriteHelper())->rewriteFileTags(
            new InputFileOptions(
                $configuration,
                $clientWrapper->hasBranch(),
                (string) $clientWrapper->getClientOptionsReadOnly()->getRunId()
            ),
            $clientWrapper,
            $testLogger
        )->getDefinition();
        // it should NOT rewrite the include tag because there is no branch file that exists
        // but it SHOULD rewrite the processed tag for this branch
        self::assertEquals(
            [[
                'name' => self::TEST_REWRITE_BASE_TAG,
                'match' => 'include',
            ], [
                'name' => $branchProcessedTag,
                'match' => 'exclude',
            ]],
            $expectedConfiguration['source']['tags']
        );
    }
}
