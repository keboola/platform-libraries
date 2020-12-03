<?php

namespace Keboola\OutputMapping\Writer;

use Keboola\InputMapping\Reader\WorkspaceProviderInterface;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

abstract class AbstractWriter
{
    /**
     * @var ClientWrapper
     */
    protected $clientWrapper;

    /**
     * @var LoggerInterface
     */
    protected $logger;

    /**
     * @var string
     */
    protected $format = 'json';

    /**
     * @var WorkspaceProviderInterface
     */
    protected $workspaceProvider;

    /**
     * AbstractWriter constructor.
     *
     * @param ClientWrapper $clientWrapper
     * @param LoggerInterface $logger
     */
    public function __construct(
        ClientWrapper $clientWrapper,
        LoggerInterface $logger,
        WorkspaceProviderInterface $workspaceProvider
    ) {
        $this->clientWrapper = $clientWrapper;
        $this->logger = $logger;
        $this->workspaceProvider = $workspaceProvider;
    }

    /**
     * @param string $format
     */
    public function setFormat($format)
    {
        $this->format = $format;
    }
}
