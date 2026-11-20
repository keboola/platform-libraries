<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Mapping;

use Keboola\OutputMapping\Storage\TableInfo;

class MappingTableLayout
{
    private const TIME_PARTITIONING = 'time_partitioning';
    private const RANGE_PARTITIONING = 'range_partitioning';
    private const CLUSTERING = 'clustering';

    /**
     * @param array<string, mixed>|null $timePartitioning
     * @param array<string, mixed>|null $rangePartitioning
     * @param array<string, mixed>|null $clustering
     */
    public function __construct(
        private readonly ?array $timePartitioning,
        private readonly ?array $rangePartitioning,
        private readonly ?array $clustering,
    ) {
    }

    public function isEmpty(): bool
    {
        return $this->timePartitioning === null && $this->rangePartitioning === null && $this->clustering === null;
    }

    /**
     * @return string[]
     */
    public function getConfiguredKeys(): array
    {
        $keys = [];
        if ($this->timePartitioning !== null) {
            $keys[] = self::TIME_PARTITIONING;
        }
        if ($this->rangePartitioning !== null) {
            $keys[] = self::RANGE_PARTITIONING;
        }
        if ($this->clustering !== null) {
            $keys[] = self::CLUSTERING;
        }

        return $keys;
    }

    /**
     * @return array<string, mixed>
     */
    public function toRequestData(): array
    {
        $data = [];
        if ($this->timePartitioning !== null) {
            $timePartitioning = ['type' => $this->timePartitioning['type'] ?? null];
            if (isset($this->timePartitioning['field'])) {
                $timePartitioning['field'] = $this->timePartitioning['field'];
            }
            if (isset($this->timePartitioning['expiration_ms'])) {
                $timePartitioning['expirationMs'] = $this->timePartitioning['expiration_ms'];
            }
            $data['timePartitioning'] = $timePartitioning;
        }
        if ($this->rangePartitioning !== null) {
            $range = self::toArray($this->rangePartitioning['range'] ?? null);
            $data['rangePartitioning'] = [
                'field' => $this->rangePartitioning['field'] ?? null,
                'range' => [
                    'start' => $range['start'] ?? null,
                    'end' => $range['end'] ?? null,
                    'interval' => $range['interval'] ?? null,
                ],
            ];
        }
        if ($this->clustering !== null) {
            $data['clustering'] = [
                'fields' => array_values(self::toArray($this->clustering['fields'] ?? null)),
            ];
        }

        return $data;
    }

    /**
     * @return string[] configured keys whose value differs from the stored layout of the table
     */
    public function findDifferences(TableInfo $table): array
    {
        $stored = $table->getLayout();
        $differences = [];

        if ($this->timePartitioning !== null
            && !$this->isSameTimePartitioning($this->timePartitioning, $stored['timePartitioning'] ?? null)
        ) {
            $differences[] = self::TIME_PARTITIONING;
        }
        if ($this->rangePartitioning !== null
            && !$this->isSameRangePartitioning($this->rangePartitioning, $stored['rangePartitioning'] ?? null)
        ) {
            $differences[] = self::RANGE_PARTITIONING;
        }
        if ($this->clustering !== null && !$this->isSameClustering($this->clustering, $stored['clustering'] ?? null)) {
            $differences[] = self::CLUSTERING;
        }

        return $differences;
    }

    public function describeRequested(): string
    {
        return (string) json_encode($this->toRequestData());
    }

    /**
     * @param array<string, mixed> $configured
     */
    private function isSameTimePartitioning(array $configured, mixed $stored): bool
    {
        if (!is_array($stored)) {
            return false;
        }
        if (strcasecmp(self::toString($configured['type'] ?? null), self::toString($stored['type'] ?? null)) !== 0) {
            return false;
        }
        if (self::toString($configured['field'] ?? null) !== self::toString($stored['field'] ?? null)) {
            return false;
        }

        return !isset($configured['expiration_ms'])
            || self::toString($configured['expiration_ms']) === self::toString($stored['expirationMs'] ?? null);
    }

    /**
     * @param array<string, mixed> $configured
     */
    private function isSameRangePartitioning(array $configured, mixed $stored): bool
    {
        if (!is_array($stored)) {
            return false;
        }
        if (self::toString($configured['field'] ?? null) !== self::toString($stored['field'] ?? null)) {
            return false;
        }

        $configuredRange = self::toArray($configured['range'] ?? null);
        $storedRange = self::toArray($stored['range'] ?? null);
        foreach (['start', 'end', 'interval'] as $key) {
            if (self::toString($configuredRange[$key] ?? null) !== self::toString($storedRange[$key] ?? null)) {
                return false;
            }
        }

        return true;
    }

    /**
     * @param array<string, mixed> $configured
     */
    private function isSameClustering(array $configured, mixed $stored): bool
    {
        if (!is_array($stored)) {
            return false;
        }

        return self::toStringList($configured['fields'] ?? null) === self::toStringList($stored['fields'] ?? null);
    }

    /**
     * @return array<mixed>
     */
    private static function toArray(mixed $value): array
    {
        return is_array($value) ? $value : [];
    }

    /**
     * @return string[]
     */
    private static function toStringList(mixed $value): array
    {
        return array_values(array_map(self::toString(...), self::toArray($value)));
    }

    private static function toString(mixed $value): string
    {
        return is_scalar($value) ? (string) $value : '';
    }
}
