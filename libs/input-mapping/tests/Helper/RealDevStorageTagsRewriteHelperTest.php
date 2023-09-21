<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Helper;

use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\File\Options\InputFileOptions;
use Keboola\InputMapping\File\Options\RewrittenInputFileOptions;
use Keboola\InputMapping\Helper\RealDevStorageTagsRewriteHelper;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;

class RealDevStorageTagsRewriteHelperTest extends TestCase
{
    private const TEST_REWRITE_BASE_TAG = 'im-files-test';

    private static string $branchId;
    protected string $tmpDir;

    public function setUp(): void
    {
        parent::setUp();

        // Create folders
        $temp = new Temp('docker');
        $this->tmpDir = $temp->getTmpFolder();
        sleep(2);
        $clientWrapper = self::getClientWrapper(self::$branchId);
        $files = $clientWrapper->getBasicClient()->listFiles(
            (new ListFilesOptions())->setTags([self::TEST_REWRITE_BASE_TAG]),
        );
        foreach ($files as $file) {
            $clientWrapper->getBasicClient()->deleteFile($file['id']);
        }
        $files = $clientWrapper->getBranchClient()->listFiles(
            (new ListFilesOptions())->setTags([self::TEST_REWRITE_BASE_TAG]),
        );
        foreach ($files as $file) {
            $clientWrapper->getBranchClient()->deleteFile($file['id']);
        }
    }

    public static function setUpBeforeClass(): void
    {
        parent::setUpBeforeClass();
        $branchesApi = new DevBranches(self::getClientWrapper(null)->getBasicClient());
        self::$branchId = (string) $branchesApi->createBranch(uniqid('TagsRewriteHelperTest'))['id'];
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
                $branchId,
            ),
        );
    }

    public function testNoBranch(): void
    {
        $configuration = ['tags' => [self::TEST_REWRITE_BASE_TAG]];
        $clientWrapper = self::getClientWrapper(null);
        $testLogger = new TestLogger();
        $processedConfiguration = (new RealDevStorageTagsRewriteHelper())->rewriteFileTags(
            new InputFileOptions(
                $configuration,
                $clientWrapper->isDevelopmentBranch(),
                (string) $clientWrapper->getClientOptionsReadOnly()->getRunId(),
            ),
            $clientWrapper,
            $testLogger,
        );

        self::assertEquals(
            new RewrittenInputFileOptions(
                ['tags' => [self::TEST_REWRITE_BASE_TAG]],
                false,
                '',
                ['tags' => [self::TEST_REWRITE_BASE_TAG], 'overwrite' => true],
                (int) $clientWrapper->getDefaultBranch()->id,
            ),
            $processedConfiguration,
        );
    }

    public function testBranchRewriteFilesExists(): void
    {
        $root = $this->tmpDir;
        file_put_contents($root . '/upload', 'test');
        $clientWrapper = self::getClientWrapper(self::$branchId);
        $clientWrapper->getBranchClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([self::TEST_REWRITE_BASE_TAG]),
        );
        sleep(2);

        $configuration = ['tags' => ['im-files-test']];

        $configuration = new InputFileOptions(
            $configuration,
            $clientWrapper->isDevelopmentBranch(),
            (string) $clientWrapper->getClientOptionsReadOnly()->getRunId(),
        );

        $testLogger = new TestLogger();
        $processedConfiguration = (new RealDevStorageTagsRewriteHelper())->rewriteFileTags(
            $configuration,
            $clientWrapper,
            $testLogger,
        );

        self::assertEquals(
            new RewrittenInputFileOptions(
                ['tags' => [self::TEST_REWRITE_BASE_TAG]],
                true,
                '',
                ['tags' => [self::TEST_REWRITE_BASE_TAG], 'overwrite' => true],
                (int) self::$branchId,
            ),
            $processedConfiguration,
        );
        self::assertTrue($testLogger->hasInfoThatContains(
            sprintf('Using files from development branch "%s" for tags "im-files-test".', self::$branchId),
        ));
    }

    public function testBranchRewriteSourceTagsFilesExists(): void
    {
        $root = $this->tmpDir;
        file_put_contents($root . '/upload', 'test');

        $clientWrapper = self::getClientWrapper(self::$branchId);
        $clientWrapper
            ->getBranchClient()
            ->uploadFile($root . '/upload', (new FileUploadOptions())->setTags([self::TEST_REWRITE_BASE_TAG]));
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
        $processedConfiguration = (new RealDevStorageTagsRewriteHelper())->rewriteFileTags(
            new InputFileOptions(
                $configuration,
                $clientWrapper->isDevelopmentBranch(),
                (string) $clientWrapper->getClientOptionsReadOnly()->getRunId(),
            ),
            $clientWrapper,
            $testLogger,
        );

        self::assertEquals(
            new RewrittenInputFileOptions(
                [
                    'source' => [
                        'tags' => [
                            [
                                'name' => self::TEST_REWRITE_BASE_TAG,
                                'match' => 'include',
                            ],
                        ],
                    ],
                ],
                true,
                '',
                [
                    'source' => [
                        'tags' => [
                            [
                                'name' => self::TEST_REWRITE_BASE_TAG,
                                'match' => 'include',
                            ],
                        ],
                    ],
                    'overwrite' => true,
                ],
                (int) self::$branchId,
            ),
            $processedConfiguration,
        );
        self::assertTrue(
            $testLogger->hasInfoThatContains(
                sprintf(
                    'Using files from development branch "%s" for tags "im-files-test".',
                    self::$branchId,
                ),
            ),
        );
    }

    public function testBranchRewriteNoFiles(): void
    {
        $testLogger = new TestLogger();
        sleep(2); // wait for storage files cleanup to take effect
        $clientWrapper = self::getClientWrapper(self::$branchId);
        $processedConfiguration = (new RealDevStorageTagsRewriteHelper())->rewriteFileTags(
            new InputFileOptions(
                ['tags' => [self::TEST_REWRITE_BASE_TAG]],
                $clientWrapper->isDevelopmentBranch(),
                (string) $clientWrapper->getClientOptionsReadOnly()->getRunId(),
            ),
            $clientWrapper,
            $testLogger,
        );

        self::assertEquals(
            new RewrittenInputFileOptions(
                [
                    'tags' => [self::TEST_REWRITE_BASE_TAG],
                ],
                true,
                '',
                [
                    'tags' => [self::TEST_REWRITE_BASE_TAG],
                    'overwrite' => true,
                ],
                (int) $clientWrapper->getDefaultBranch()->id,
            ),
            $processedConfiguration,
        );
        self::assertTrue(
            $testLogger->hasInfoThatContains(
                sprintf(
                    'Using files from default branch "%s" for tags "im-files-test".',
                    $clientWrapper->getDefaultBranch()->id,
                ),
            ),
        );
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
        $processedConfiguration = (new RealDevStorageTagsRewriteHelper())->rewriteFileTags(
            new InputFileOptions(
                $configuration,
                $clientWrapper->isDevelopmentBranch(),
                (string) $clientWrapper->getClientOptionsReadOnly()->getRunId(),
            ),
            $clientWrapper,
            $testLogger,
        );

        self::assertEquals(
            new RewrittenInputFileOptions(
                [
                    'source' => [
                        'tags' => [
                            [
                                'name' => self::TEST_REWRITE_BASE_TAG,
                                'match' => 'include',
                            ],
                        ],
                    ],
                ],
                true,
                '',
                [
                    'source' => [
                        'tags' => [
                            [
                                'name' => self::TEST_REWRITE_BASE_TAG,
                                'match' => 'include',
                            ],
                        ],
                    ],
                    'overwrite' => true,
                ],
                (int) $clientWrapper->getDefaultBranch()->id,
            ),
            $processedConfiguration,
        );
        self::assertTrue(
            $testLogger->hasInfoThatContains(
                sprintf(
                    'Using files from default branch "%s" for tags "im-files-test".',
                    $clientWrapper->getDefaultBranch()->id,
                ),
            ),
        );
    }

    public function testBranchProcessedTagsSourceTag(): void
    {
        $clientWrapper = self::getClientWrapper(self::$branchId);
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
        $this->expectException(InvalidInputException::class);
        $this->expectExceptionMessage('The "processed_tags" property is not supported for development storage.');
        (new RealDevStorageTagsRewriteHelper())->rewriteFileTags(
            new InputFileOptions(
                $configuration,
                $clientWrapper->isDevelopmentBranch(),
                (string) $clientWrapper->getClientOptionsReadOnly()->getRunId(),
            ),
            $clientWrapper,
            $testLogger,
        );
    }
}
