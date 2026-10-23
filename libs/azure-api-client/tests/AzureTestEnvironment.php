<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests;

use Keboola\AzureApiClient\ApiClientConfiguration;
use Keboola\AzureApiClient\Authentication\Authenticator\Internal\CustomHeaderAuthenticator;
use Keboola\AzureApiClient\Authentication\Authenticator\RequestAuthenticatorFactoryInterface;
use Keboola\AzureApiClient\Authentication\Authenticator\RequestAuthenticatorInterface;
use Keboola\AzureApiClient\Authentication\Authenticator\SharedAccessSignatureAuth;
use Keboola\AzureApiClient\EventGrid\Resources as EventGridResources;
use Keboola\AzureApiClient\ServiceBus\Model\ReceivedMessage;
use Keboola\AzureApiClient\ServiceBus\Resources as ServiceBusResources;
use Keboola\AzureApiClient\ServiceBus\ServiceBusApiClient;
use RuntimeException;

/**
 * Access to the Azure resources provisioned by provisioning/ for tests in the "azure" group.
 */
final class AzureTestEnvironment
{
    private const ENV_PREFIX = 'AZURE_API_CLIENT_TEST__';
    private const RECEIVE_WAIT_SECONDS = 5;

    /**
     * @return non-empty-string
     */
    public static function get(string $name): string
    {
        $value = getenv(self::ENV_PREFIX . $name);
        if ($value === false || $value === '') {
            throw new RuntimeException(sprintf(
                'Environment variable "%s" is not set, see "Azure resources" in README.md',
                self::ENV_PREFIX . $name,
            ));
        }
        return $value;
    }

    /**
     * Bearer authentication with Entra tokens issued per resource by provisioning/write-env.sh, so a client asking
     * for any other resource than the one its service expects fails.
     */
    public static function entraTokenAuth(): RequestAuthenticatorFactoryInterface
    {
        $tokens = [
            ServiceBusResources::AZURE_SERVICE_BUS => self::get('SERVICE_BUS_ENTRA_TOKEN'),
            EventGridResources::AZURE_EVENT_GRID => self::get('EVENT_GRID_ENTRA_TOKEN'),
        ];

        return new class ($tokens) implements RequestAuthenticatorFactoryInterface {
            /** @param array<string, string> $tokens */
            public function __construct(private readonly array $tokens)
            {
            }

            public function createRequestAuthenticator(string $resource): RequestAuthenticatorInterface
            {
                if (!array_key_exists($resource, $this->tokens)) {
                    throw new RuntimeException(sprintf('No Entra token for resource "%s"', $resource));
                }
                return new CustomHeaderAuthenticator('Authorization', 'Bearer ' . $this->tokens[$resource]);
            }
        };
    }

    public static function serviceBusWithSas(): ServiceBusApiClient
    {
        return new ServiceBusApiClient(
            self::get('SERVICE_BUS_ENDPOINT'),
            new ApiClientConfiguration(
                authenticator: new SharedAccessSignatureAuth(
                    self::get('SERVICE_BUS_SAS_KEY_NAME'),
                    self::get('SERVICE_BUS_SAS_KEY'),
                ),
            ),
        );
    }

    /**
     * Receives from the queue until a message matching $predicate arrives. Messages that do not match are
     * leftovers of earlier runs and are deleted.
     *
     * @param callable(ReceivedMessage): bool $predicate
     */
    public static function receiveMatching(
        ServiceBusApiClient $client,
        string $queueName,
        callable $predicate,
        int $timeoutSeconds,
    ): ReceivedMessage {
        $deadline = time() + $timeoutSeconds;
        do {
            $message = $client->peekLockMessage($queueName, self::RECEIVE_WAIT_SECONDS);
            if ($message !== null) {
                if ($predicate($message)) {
                    return $message;
                }
                $client->deleteMessage($message->lockLocation);
            }
        } while (time() < $deadline);

        throw new RuntimeException(sprintf(
            'No matching message arrived to queue "%s" within %d seconds',
            $queueName,
            $timeoutSeconds,
        ));
    }
}
