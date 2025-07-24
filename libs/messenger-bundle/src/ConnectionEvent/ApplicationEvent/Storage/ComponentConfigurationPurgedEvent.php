<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\ConnectionEvent\ApplicationEvent\Storage;

use InvalidArgumentException;
use Keboola\MessengerBundle\ConnectionEvent\EventInterface;

class ComponentConfigurationPurgedEvent implements EventInterface
{
    public const NAME = 'storage.componentConfigurationPurged';

    public function __construct(
        public readonly string $uuid,
        public readonly int $projectId,

        public readonly int $accessTokenId,
        public readonly string $accessTokenName,

        public readonly string|int $objectId,
        public readonly string $objectType,
        public readonly string $objectName,

        public readonly string $message,
        public readonly array $params,

        public readonly int $idBranch,
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
            $data['idAccessToken'],
            $data['accessTokenName'],
            $data['objectId'],
            $data['objectType'],
            $data['objectName'],
            $data['message'],
            $data['params'],
            $data['idBranch'],
        );
    }

    public function toArray(): array
    {
        return [
            'name' => self::NAME,
            'uuid' => $this->uuid,
            'idProject' => $this->projectId,
            'idAccessToken' => $this->accessTokenId,
            'accessTokenName' => $this->accessTokenName,
            'objectId' => $this->objectId,
            'objectType' => $this->objectType,
            'objectName' => $this->objectName,
            'message' => $this->message,
            'params' => $this->params,
            'idBranch' => $this->idBranch,
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

    public function getComponent(): string
    {
        return $this->params['component'];
    }

    public function getConfigurationId(): string
    {
        return $this->params['configurationId'];
    }
}
