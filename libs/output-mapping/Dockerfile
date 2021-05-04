ARG PHP_VERSION=7.4
FROM php:${PHP_VERSION} as dev

ARG XDEBUG_VERSION=2.9.8
ENV COMPOSER_ALLOW_SUPERUSER 1

WORKDIR /code

#RUN apt-get update -q \
# && apt-get install unzip git zlib1g-dev -y

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      git \
      unzip \
      libzip-dev \
      zlib1g-dev \
 && rm -r /var/lib/apt/lists/*

RUN curl -sS https://getcomposer.org/installer | php -- --install-dir=/usr/local/bin/ --filename=composer

# install extensions
RUN pecl config-set php_ini /usr/local/etc/php.ini \
    && yes | pecl install "xdebug-${XDEBUG_VERSION}" \
    && echo "zend_extension=$(find /usr/local/lib/php/extensions/ -name xdebug.so)" > /usr/local/etc/php/conf.d/xdebug.ini

RUN docker-php-ext-install zip

FROM dev as tests
COPY composer.* ./
RUN composer install $COMPOSER_FLAGS --no-scripts --no-autoloader
COPY . .
RUN composer install $COMPOSER_FLAGS
