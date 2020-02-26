FROM php:5.6-cli
ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update -q \
  && apt-get install unzip git zlib1g-dev libzip-dev -y

RUN docker-php-ext-install zip

RUN cd \
  && curl -sS https://getcomposer.org/installer | php \
  && ln -s /root/composer.phar /usr/local/bin/composer

# install extensions
RUN pecl config-set php_ini /usr/local/etc/php.ini \
    && yes | pecl install xdebug-2.5.5 \
    && echo "zend_extension=$(find /usr/local/lib/php/extensions/ -name xdebug.so)" > /usr/local/etc/php/conf.d/xdebug.ini


WORKDIR /code
