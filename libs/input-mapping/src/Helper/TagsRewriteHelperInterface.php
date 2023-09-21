<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Helper;

use Keboola\InputMapping\File\Options\InputFileOptions;
use Keboola\InputMapping\File\Options\RewrittenInputFileOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

interface TagsRewriteHelperInterface
{
    public function rewriteFileTags(
        InputFileOptions $fileConfigurationOriginal,
        ClientWrapper $clientWrapper,
        LoggerInterface $logger,
    ): RewrittenInputFileOptions;
}
