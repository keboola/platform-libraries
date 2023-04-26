<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Security;

use Keboola\ApiBundle\Attribute\AuthAttributeInterface;
use Keboola\ApiBundle\Attribute\ManageTokenAuth;
use Keboola\ApiBundle\Attribute\StorageTokenAuth;
use Keboola\ApiBundle\Security\ManageToken\ManageTokenAuthenticator;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiTokenAuthenticator;
use Keboola\ApiBundle\Util\ControllerReflector;
use Keboola\ErrorControl\ErrorResponse;
use Psr\Container\ContainerInterface;
use ReflectionAttribute;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\HttpKernel\Exception\HttpException;
use Symfony\Component\Security\Core\Authentication\Token\TokenInterface;
use Symfony\Component\Security\Core\Exception\AccessDeniedException;
use Symfony\Component\Security\Core\Exception\AuthenticationException;
use Symfony\Component\Security\Core\Exception\CustomUserMessageAuthenticationException;
use Symfony\Component\Security\Http\Authenticator\AbstractAuthenticator;
use Symfony\Component\Security\Http\Authenticator\Passport\Badge\UserBadge;
use Symfony\Component\Security\Http\Authenticator\Passport\Passport;
use Symfony\Component\Security\Http\Authenticator\Passport\SelfValidatingPassport;
use Symfony\Contracts\Service\ServiceSubscriberInterface;

class AttributeAuthenticator extends AbstractAuthenticator implements ServiceSubscriberInterface
{
    public function __construct(
        private readonly ControllerReflector $controllerReflector,
        private readonly ContainerInterface $authenticators,
    ) {
    }

    /**
     * //return array<class-string<AuthAttributeInterface>, class-string<TokenAuthenticatorInterface>>
     */
    public static function getSubscribedServices(): array
    {
        return [
            StorageTokenAuth::class => StorageApiTokenAuthenticator::class,
            ManageTokenAuth::class => ManageTokenAuthenticator::class,
        ];
    }

    public function supports(Request $request): ?bool
    {
        return count($this->getControllerAuthAttributes($request)) > 0;
    }

    public function authenticate(Request $request): Passport
    {
        $authAttributes = $this->getControllerAuthAttributes($request);

        foreach ($authAttributes as $authAttribute) {
            $authenticator = $this->authenticators->get($authAttribute->getName());
            assert($authenticator instanceof TokenAuthenticatorInterface);

            $tokenHeader = $authenticator->getTokenHeader();
            $token = $request->headers->get($tokenHeader);

            if ($token === null) {
                throw new CustomUserMessageAuthenticationException(sprintf(
                    'Authentication header "%s" is missing',
                    $tokenHeader,
                ));
            }

            $authAttributeInstance = $authAttribute->newInstance();

            try {
                $authorizedToken = $authenticator->authenticateToken($authAttributeInstance, $token);
            } catch (AuthenticationException $error) {
                continue;
            }

            try {
                $authenticator->authorizeToken($authAttributeInstance, $authorizedToken);
            } catch (AccessDeniedException $e) {
                throw new CustomUserMessageAuthenticationException(
                    $e->getMessage(),
                    [],
                    Response::HTTP_FORBIDDEN,
                    $e
                );
            }

            $userBadge = new UserBadge(
                $authorizedToken->getUserIdentifier(),
                fn () => $authorizedToken,
            );

            return new SelfValidatingPassport($userBadge);
        }

        throw $error ?? new CustomUserMessageAuthenticationException('No API token provided');
    }

    public function onAuthenticationSuccess(Request $request, TokenInterface $token, string $firewallName): ?Response
    {
        return null;
    }

    public function onAuthenticationFailure(Request $request, AuthenticationException $exception): ?Response
    {
        $errorMessage = $exception instanceof CustomUserMessageAuthenticationException ?
            $exception->getMessage() :
            'Authorization failed';

        return ErrorResponse::fromException(new HttpException(
            $exception->getCode() > 0 ? $exception->getCode() : Response::HTTP_UNAUTHORIZED,
            $errorMessage,
            $exception
        ));
    }

    /**
     * @return list<ReflectionAttribute<AuthAttributeInterface>>
     */
    private function getControllerAuthAttributes(Request $request): array
    {
        $controller = $request->attributes->get('_controller');
        if ($controller === null) {
            return [];
        }

        $controllerMethodReflection = $this->controllerReflector->resolveControllerMethod($controller);
        if ($controllerMethodReflection === null) {
            return [];
        }

        return $controllerMethodReflection->getDeclaringClass()->getAttributes(
            AuthAttributeInterface::class,
            ReflectionAttribute::IS_INSTANCEOF,
        );
    }
}
