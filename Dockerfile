# syntax=docker/dockerfile:1.3

ARG PHP_VERSION=7.4

FROM php:${PHP_VERSION}-cli AS base

ARG DEBIAN_FRONTEND=noninteractive
ENV COMPOSER_FLAGS="--prefer-dist --no-interaction"
ENV COMPOSER_ALLOW_SUPERUSER 1

COPY docker/php/php.ini /usr/local/etc/php/php.ini

RUN apt-get update && apt-get install -y \
        git \
        unzip \
        zlib1g-dev \
        libzip-dev \
	--no-install-recommends && rm -r /var/lib/apt/lists/*

RUN curl -sS https://getcomposer.org/installer | php -- --install-dir=/usr/local/bin/ --filename=composer

RUN pecl channel-update pecl.php.net \
    && pecl config-set php_ini /usr/local/etc/php.ini \
    && pecl install xdebug-beta \
    && docker-php-ext-enable xdebug \
	&& docker-php-ext-install zip

FROM base AS input-mapping

ENV LIB_NAME=input-mapping
ENV LIB_HOME=/code/libs/${LIB_NAME}

WORKDIR ${LIB_HOME}

COPY libs/${LIB_NAME}/composer.* ./
RUN composer install $COMPOSER_FLAGS --no-scripts --no-autoloader
COPY libs/${LIB_NAME} ./
RUN composer install $COMPOSER_FLAGS

