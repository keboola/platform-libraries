<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\ConnectionEvent\ApplicationEvent\Admin;

use InvalidArgumentException;
use Keboola\MessengerBundle\ConnectionEvent\EventInterface;

class AdminRemovedFromProjectEvent implements EventInterface
{
    public const NAME = 'admin.adminRemovedFromProject';

    public function __construct(
        public readonly string $uuid,
        public readonly int $projectId,
        public readonly int $adminId,

        public readonly string|int $objectId,
        public readonly string $objectType,
        public readonly string $objectName,

        public readonly array $params,
    ) {
    }

    public static function fromArray(array $data): self
    {
        if ($data['name'] !== self::NAME) {
            throw new InvalidArgumentException(sprintf(
                '%s expects event name "%s" but is "%s"',
                static::class,
                self::NAME,
                $data['name'],
            ));
        }

        return new self(
            $data['uuid'],
            $data['idProject'],
            $data['idAdmin'],
            $data['objectId'],
            $data['objectType'],
            $data['objectName'],
            $data['params'],
        );
    }

    public function toArray(): array
    {
        return [
            'name' => self::NAME,
            'uuid' => $this->uuid,
            'idProject' => $this->projectId,
            'idAdmin' => $this->adminId,
            'objectId' => $this->objectId,
            'objectType' => $this->objectType,
            'objectName' => $this->objectName,
            'params' => $this->params,
        ];
    }

    public function getId(): string
    {
        return $this->uuid;
    }

    public function getEventName(): string
    {
        return self::NAME;
    }
}
