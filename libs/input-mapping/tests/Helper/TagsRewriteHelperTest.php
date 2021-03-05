<?php

namespace Keboola\InputMapping\Tests\Helper;

use Keboola\InputMapping\Helper\TagsRewriteHelper;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;

class TagsRewriteHelperTest extends TestCase
{
    const TEST_REWRITE_BASE_TAG = 'im-files-test';

    /** @var ClientWrapper */
    private $clientWrapper;

    /** @var string */
    private $branchId;

    /** @var string */
    private $branchTag;

    /** @var string */
    protected $tmpDir;

    public function setUp()
    {
        parent::setUp();

        // Create folders
        $temp = new Temp('docker');
        $temp->initRunFolder();
        $this->temp = $temp;
        $this->tmpDir = $temp->getTmpFolder();

        $this->clientWrapper = new ClientWrapper(
            new Client(['token' => STORAGE_API_TOKEN_MASTER, "url" => STORAGE_API_URL]),
            null,
            null
        );

        $branches = new DevBranches($this->clientWrapper->getBasicClient());
        foreach ($branches->listBranches() as $branch) {
            if ($branch['name'] === 'dev branch') {
                $branches->deleteBranch($branch['id']);
            }
        }
        $this->branchId = $branches->createBranch('dev branch')['id'];
        $this->branchTag = sprintf('%s-' . self::TEST_REWRITE_BASE_TAG, $this->branchId);
    }

    protected function tearDown()
    {
        sleep(2);
        $files = $this->clientWrapper->getBasicClient()->listFiles(
            (new ListFilesOptions())->setTags([self::TEST_REWRITE_BASE_TAG, $this->branchTag])
        );
        foreach ($files as $file) {
            $this->clientWrapper->getBasicClient()->deleteFile($file['id']);
        }
        parent::tearDown();
    }

    public function testNoBranch()
    {
        $configuration = ['tags' => [self::TEST_REWRITE_BASE_TAG]];

        $this->clientWrapper->setBranchId('');

        $testLogger = new TestLogger();
        $expectedConfiguration = TagsRewriteHelper::rewriteFileTags($configuration, $this->clientWrapper, $testLogger);

        self::assertSame($configuration, $expectedConfiguration);
    }

    public function testBranchRewriteFilesExists()
    {
        $root = $this->tmpDir;
        file_put_contents($root . '/upload', 'test');

        $this->clientWrapper->getBasicClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$this->branchTag])
        );
        sleep(5);

        $this->clientWrapper->setBranchId($this->branchId);

        $configuration = ['tags' => ['im-files-test']];

        $testLogger = new TestLogger();
        $expectedConfiguration = TagsRewriteHelper::rewriteFileTags($configuration, $this->clientWrapper, $testLogger);

        self::assertTrue($testLogger->hasInfoThatContains(
            sprintf('Using dev tags "%s" instead of "' . self::TEST_REWRITE_BASE_TAG . '".', $this->branchTag)
        ));

        self::assertEquals([$this->branchTag], $expectedConfiguration['tags']);
    }

    public function testBranchRewriteSourceTagsFilesExists()
    {
        $root = $this->tmpDir;
        file_put_contents($root . '/upload', 'test');

        $this->clientWrapper
            ->getBasicClient()
            ->uploadFile($root . '/upload', (new FileUploadOptions())->setTags([$this->branchTag]));
        sleep(5);

        $this->clientWrapper->setBranchId($this->branchId);

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
            $this->clientWrapper,
            $testLogger
        );
        self::assertTrue(
            $testLogger->hasInfoThatContains(
                sprintf(
                    'Using dev source tags "%s" instead of "' . self::TEST_REWRITE_BASE_TAG . '".',
                    $this->branchTag
                )
            )
        );

        self::assertEquals(
            [[
                'name' => $this->branchTag,
                'match' => 'include',
            ]],
            $expectedConfiguration['source']['tags']
        );
    }

    public function testBranchRewriteNoFiles()
    {
        $configuration = ['tags' => [self::TEST_REWRITE_BASE_TAG]];

        $this->clientWrapper->setBranchId($this->branchId);

        $testLogger = new TestLogger();
        $expectedConfiguration = TagsRewriteHelper::rewriteFileTags($configuration, $this->clientWrapper, $testLogger);

        self::assertFalse($testLogger->hasInfoThatContains(
            sprintf('Using dev tags "%s" instead of "' . self::TEST_REWRITE_BASE_TAG . '".', $this->branchTag)
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

        $this->clientWrapper->setBranchId($this->branchId);

        $testLogger = new TestLogger();
        $expectedConfiguration = TagsRewriteHelper::rewriteFileTags(
            $configuration,
            $this->clientWrapper,
            $testLogger
        );

        self::assertFalse(
            $testLogger->hasInfoThatContains(
                sprintf('Using dev tags "%s" instead of "' . self::TEST_REWRITE_BASE_TAG . '".', $this->branchTag)
            )
        );

        self::assertEquals($configuration, $expectedConfiguration);
    }

    public function testBranchRewriteExcludedProcessedSourceTagFilesExist()
    {
        $branchProcessedTag = sprintf('%s-processed', $this->branchId);
        file_put_contents($this->tmpDir . '/upload', 'test');
        $this->clientWrapper
            ->getBasicClient()
            ->uploadFile(
                $this->tmpDir . '/upload',
                (new FileUploadOptions())->setTags([$this->branchTag, $branchProcessedTag])
            );
        sleep(2);
        $configuration = [
            'source' => [
                'tags' => [
                    [
                        'name' => $this->branchTag,
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
        $this->clientWrapper->setBranchId($this->branchId);
        $testLogger = new TestLogger();
        $expectedConfiguration = TagsRewriteHelper::rewriteFileTags(
            $configuration,
            $this->clientWrapper,
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
        $branchProcessedTag = sprintf('%s-processed', $this->branchId);
        file_put_contents($this->tmpDir . '/upload', 'test');
        $this->clientWrapper
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
        $this->clientWrapper->setBranchId($this->branchId);
        $testLogger = new TestLogger();
        $expectedConfiguration = TagsRewriteHelper::rewriteFileTags(
            $configuration,
            $this->clientWrapper,
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
