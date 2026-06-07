<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\Transport;

use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportFactoryInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

/**
 * Decorates GpsTransportFactory to inject a default REST client timeout. Without it, the Pub/Sub long-poll
 * pull() has no client-side timeout and a half-open TCP connection blocks the consumer forever (AJDA-2866).
 *
 * @implements TransportFactoryInterface<TransportInterface>
 */
final class GpsTransportFactoryDecorator implements TransportFactoryInterface
{
    // 120s > the ~90s Pub/Sub server-side long-poll hold, so a healthy poll is never cut short
    public const DEFAULT_REST_OPTIONS = [
        'timeout' => 120,
        'connect_timeout' => 10,
    ];

    /**
     * @param TransportFactoryInterface<TransportInterface> $inner
     */
    public function __construct(
        private readonly TransportFactoryInterface $inner,
    ) {
    }

    public function supports(string $dsn, array $options): bool
    {
        return $this->inner->supports($dsn, $options);
    }

    public function createTransport(string $dsn, array $options, SerializerInterface $serializer): TransportInterface
    {
        // GpsConfigurationResolver merges DSN query options with $options, so an injected client_config would
        // override one configured in the DSN. Back off on any explicit client_config (whole key, not just
        // restOptions - injecting restOptions would shadow e.g. an app-configured requestTimeout).
        parse_str(parse_url($dsn, PHP_URL_QUERY) ?: '', $dsnOptions);
        if (!isset($options['client_config']) && !isset($dsnOptions['client_config'])) {
            $options['client_config'] = ['restOptions' => self::DEFAULT_REST_OPTIONS];
        }

        return $this->inner->createTransport($dsn, $options, $serializer);
    }
}
