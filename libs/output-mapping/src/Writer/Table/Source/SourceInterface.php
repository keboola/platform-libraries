<?php

namespace Keboola\OutputMapping\Writer\Table\Source;

use SplFileInfo;

interface SourceInterface
{
    /**
     * @return string
     */
    public function getSourcePathPrefix();

    /**
     * @return string
     */
    public function getSourceId();

    /**
     * @return string
     */
    public function getSourceName();

    /**
     * @param SplFileInfo $file
     */
    public function setManifestFile(SplFileInfo $file = null);

    /**
     * @return null|SplFileInfo
     */
    public function getManifestFile();

    /**
     * @param null|array $mapping
     */
    public function setMapping(array $mapping = null);

    /**
     * @return null|array
     */
    public function getMapping();
}
