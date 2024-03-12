<?php

declare(strict_types=1);

namespace Keboola\InputMapping\File\Options;

use Keboola\InputMapping\Configuration\File;
use Keboola\InputMapping\Exception\InvalidInputException;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class InputFileOptions
{
    protected array $definition;
    protected bool $isDevBranch;
    protected string $runId;

    public function __construct(array $configuration, bool $isDevBranch, string $runId)
    {
        $this->isDevBranch = $isDevBranch;
        $this->runId = $runId;

        $fileConfiguration = new File();
        try {
            $this->definition = $fileConfiguration->parse(['file' => $configuration]);
        } catch (InvalidConfigurationException $e) {
            throw new InvalidInputException($e->getMessage(), $e->getCode(), $e);
        }
        if (isset($this->definition['query']) && $this->isDevBranch) {
            throw new InvalidInputException(
                "Invalid file mapping, the 'query' attribute is unsupported in the dev/branch context.",
            );
        }
    }

    public function getDefinition(): array
    {
        return $this->definition;
    }

    public function getTags(): array
    {
        if (isset($this->definition['tags'])) {
            return $this->definition['tags'];
        }
        return [];
    }

    public function isDevBranch(): bool
    {
        return $this->isDevBranch;
    }

    public function getRunId(): string
    {
        return $this->runId;
    }
}
