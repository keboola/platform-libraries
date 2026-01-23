<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Security;

use Keboola\ApiBundle\Attribute\AuthAttributeInterface;
use Keboola\ApiBundle\Util\ControllerReflector;
use Keboola\ErrorControl\ErrorResponse;
use Psr\Container\ContainerInterface;
use ReflectionAttribute;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\HttpKernel\Exception\HttpException;
use Symfony\Component\Security\Core\Authentication\Token\TokenInterface as SymfonyTokenInterface;
use Symfony\Component\Security\Core\Exception\AccessDeniedException;
use Symfony\Component\Security\Core\Exception\AuthenticationException;
use Symfony\Component\Security\Core\Exception\CustomUserMessageAuthenticationException;
use Symfony\Component\Security\Http\Authenticator\AbstractAuthenticator;
use Symfony\Component\Security\Http\Authenticator\Passport\Badge\UserBadge;
use Symfony\Component\Security\Http\Authenticator\Passport\Passport;
use Symfony\Component\Security\Http\Authenticator\Passport\SelfValidatingPassport;

class AttributeAuthenticator extends AbstractAuthenticator
{
    public function __construct(
        private readonly ControllerReflector $controllerReflector,
        private readonly ContainerInterface $authenticators,
    ) {
    }

    public function supports(Request $request): ?bool
    {
        return count($this->getControllerAuthAttributes($request)) > 0;
    }

    public function authenticate(Request $request): SelfValidatingPassport
    {
        $authAttributes = $this->getControllerAuthAttributes($request);

        foreach ($authAttributes as $authAttribute) {
            $authenticator = $this->authenticators->get($authAttribute->getName());
            assert($authenticator instanceof TokenAuthenticatorInterface);

            $tokenHeader = $authenticator->getTokenHeader();
            $hasPrimaryHeader = $request->headers->has($tokenHeader);

            // Try to get authorization header name
            try {
                $authorizationHeaderName = $authenticator->getAuthorizationHeader();
                $hasAuthorizationHeader = $request->headers->has($authorizationHeaderName);
            } catch (AuthenticationException $e) {
                $authorizationHeaderName = null;
                $hasAuthorizationHeader = false;
            }

            // Check if both headers are present
            if ($hasPrimaryHeader && $hasAuthorizationHeader) {
                $error = new CustomUserMessageAuthenticationException(sprintf(
                    'Cannot use both "%s" and "%s" headers simultaneously',
                    $tokenHeader,
                    $authorizationHeaderName,
                ));
                continue;
            }

            // Get token from primary header or Authorization header
            $token = null;
            if ($hasPrimaryHeader) {
                $token = $request->headers->get($tokenHeader);
            } elseif ($hasAuthorizationHeader) {
                assert($authorizationHeaderName !== null);
                $token = $request->headers->get($authorizationHeaderName);
            }

            if ($token === null) {
                $errorMessage = $authorizationHeaderName !== null
                    ? sprintf(
                        'Authentication header "%s" or "%s: Bearer" is missing',
                        $tokenHeader,
                        $authorizationHeaderName,
                    )
                    : sprintf('Authentication header "%s" is missing', $tokenHeader);

                $error = new CustomUserMessageAuthenticationException($errorMessage);
                continue;
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
                $error = new CustomUserMessageAuthenticationException(
                    $e->getMessage(),
                    [],
                    Response::HTTP_FORBIDDEN,
                    $e,
                );
                continue;
            }

            $userBadge = new UserBadge(
                $authorizedToken->getUserIdentifier(),
                fn () => $authorizedToken,
            );

            return new SelfValidatingPassport($userBadge);
        }

        throw $error ?? new CustomUserMessageAuthenticationException('No API token provided');
    }

    public function onAuthenticationSuccess(
        Request $request,
        SymfonyTokenInterface $token,
        string $firewallName,
    ): ?Response {
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
            $exception,
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
