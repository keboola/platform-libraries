<?php

namespace Keboola\InputMapping\Tests\Helper;

use Keboola\InputMapping\Helper\TagsRewriteHelper;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;

class TagsRewriteHelperTest extends TestCase
{
    const TEST_REWRITE_BASE_TAG = 'im-files-test';

    private static string $branchId;
    private static string $branchTag;
    protected string $tmpDir;

    public function setUp()
    {
        parent::setUp();

        // Create folders
        $temp = new Temp('docker');
        $temp->initRunFolder();
        $this->temp = $temp;
        $this->tmpDir = $temp->getTmpFolder();
        sleep(2);
        $clientWrapper = self::getClientWrapper(null);
        $files = $clientWrapper->getBasicClient()->listFiles(
            (new ListFilesOptions())->setTags([self::TEST_REWRITE_BASE_TAG, self::$branchTag])
        );
        foreach ($files as $file) {
            $clientWrapper->getBasicClient()->deleteFile($file['id']);
        }
    }

    public static function setUpBeforeClass()
    {
        parent::setUpBeforeClass();
        $branchesApi = new DevBranches(self::getClientWrapper(null)->getBasicClient());
        self::$branchId = $branchesApi->createBranch(uniqid('TagsRewriteHelperTest'))['id'];
        self::$branchTag = sprintf('%s-' . self::TEST_REWRITE_BASE_TAG, self::$branchId);
    }

    public static function tearDownAfterClass()
    {
        $branchesApi = new DevBranches(self::getClientWrapper(null)->getBasicClient());
        $branchesApi->deleteBranch(self::$branchId);
        parent::tearDownAfterClass();
    }

    private static function getClientWrapper(?string $branchId): ClientWrapper
    {
        return new ClientWrapper(
            new ClientOptions(STORAGE_API_URL, STORAGE_API_TOKEN_MASTER, $branchId),
        );
    }

    public function testNoBranch()
    {
        $configuration = ['tags' => [self::TEST_REWRITE_BASE_TAG]];

        $testLogger = new TestLogger();
        $expectedConfiguration = TagsRewriteHelper::rewriteFileTags(
            $configuration,
            self::getClientWrapper(null),
            $testLogger
        );

        self::assertSame($configuration, $expectedConfiguration);
    }

    public function testBranchRewriteFilesExists()
    {
        $root = $this->tmpDir;
        file_put_contents($root . '/upload', 'test');
        $clientWrapper = self::getClientWrapper(self::$branchId);
        $clientWrapper->getBasicClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([self::$branchTag])
        );
        sleep(2);

        $configuration = ['tags' => ['im-files-test']];

        $testLogger = new TestLogger();
        $expectedConfiguration = TagsRewriteHelper::rewriteFileTags($configuration, $clientWrapper, $testLogger);

        self::assertTrue($testLogger->hasInfoThatContains(
            sprintf('Using dev tags "%s" instead of "' . self::TEST_REWRITE_BASE_TAG . '".', self::$branchTag)
        ));

        self::assertEquals([self::$branchTag], $expectedConfiguration['tags']);
    }

    public function testBranchRewriteSourceTagsFilesExists()
    {
        $root = $this->tmpDir;
        file_put_contents($root . '/upload', 'test');

        $clientWrapper = self::getClientWrapper(self::$branchId);
        $clientWrapper
            ->getBasicClient()
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
        $expectedConfiguration = TagsRewriteHelper::rewriteFileTags(
            $configuration,
            $clientWrapper,
            $testLogger
        );
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

    public function testBranchRewriteNoFiles()
    {
        $configuration = ['tags' => [self::TEST_REWRITE_BASE_TAG]];
        $testLogger = new TestLogger();
        $expectedConfiguration = TagsRewriteHelper::rewriteFileTags(
            $configuration,
            self::getClientWrapper(self::$branchId),
            $testLogger
        );

        self::assertFalse($testLogger->hasInfoThatContains(
            sprintf('Using dev tags "%s" instead of "' . self::TEST_REWRITE_BASE_TAG . '".', self::$branchTag)
        ));

        self::assertEquals($configuration, $expectedConfiguration);
    }

    public function testBranchRewriteSourceTagsNoFiles()
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
        $expectedConfiguration = TagsRewriteHelper::rewriteFileTags(
            $configuration,
            self::getClientWrapper(self::$branchId),
            $testLogger
        );

        self::assertFalse(
            $testLogger->hasInfoThatContains(
                sprintf('Using dev tags "%s" instead of "' . self::TEST_REWRITE_BASE_TAG . '".', self::$branchTag)
            )
        );

        self::assertEquals($configuration, $expectedConfiguration);
    }

    public function testBranchRewriteExcludedProcessedSourceTagFilesExist()
    {
        $branchProcessedTag = sprintf('%s-processed', self::$branchId);
        file_put_contents($this->tmpDir . '/upload', 'test');
        $clientWrapper = self::getClientWrapper(self::$branchId);
        $clientWrapper
            ->getBasicClient()
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
                        'match' => 'exclude'
                    ],
                ],
            ],
            'processed_tags' => ['processed'],
        ];
        $testLogger = new TestLogger();
        $expectedConfiguration = TagsRewriteHelper::rewriteFileTags(
            $configuration,
            $clientWrapper,
            $testLogger
        );
        // it should rewrite the processed exclude tag
        self::assertContains(
            [
                'name' => $branchProcessedTag,
                'match' => 'exclude',
            ],
            $expectedConfiguration['source']['tags']
        );
    }

    public function testBranchRewriteExcludedProcessedSourceTagBranchFileDoesNotExist()
    {
        $branchProcessedTag = sprintf('%s-processed', self::$branchId);
        file_put_contents($this->tmpDir . '/upload', 'test');
        $clientWrapper = self::getClientWrapper(self::$branchId);
        $clientWrapper
            ->getBasicClient()
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
                        'match' => 'exclude'
                    ],
                ],
            ],
            'processed_tags' => ['processed'],
        ];
        $testLogger = new TestLogger();
        $expectedConfiguration = TagsRewriteHelper::rewriteFileTags(
            $configuration,
            $clientWrapper,
            $testLogger
        );
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
