FROM php:7.1
MAINTAINER Erik Zigo <erik.zigo@keboola.com>
ENV DEBIAN_FRONTEND noninteractive

# Install Dependencies
RUN apt-get update \
  && apt-get install unzip git unixodbc unixodbc-dev libpq-dev -y

RUN docker-php-ext-install pdo_pgsql pdo_mysql
RUN pecl install xdebug \
  && docker-php-ext-enable xdebug

# Install PHP odbc extension
RUN set -x \
    && docker-php-source extract \
    && cd /usr/src/php/ext/odbc \
    && phpize \
    && sed -ri 's@^ *test +"\$PHP_.*" *= *"no" *&& *PHP_.*=yes *$@#&@g' configure \
    && ./configure --with-unixODBC=shared,/usr \
    && docker-php-ext-install odbc \
    && docker-php-source delete

# Snowflake ODBC
# https://github.com/docker-library/php/issues/103
RUN set -x \
    && docker-php-source extract \
    && cd /usr/src/php/ext/odbc \
    && phpize \
    && sed -ri 's@^ *test +"\$PHP_.*" *= *"no" *&& *PHP_.*=yes *$@#&@g' configure \
    && ./configure --with-unixODBC=shared,/usr \
    && docker-php-ext-install odbc \
    && docker-php-source delete

ADD ./snowflake-odbc-x86_64.deb /tmp/snowflake-odbc.deb
RUN dpkg -i /tmp/snowflake-odbc.deb
ADD ./driver/simba.snowflake.ini /usr/lib/snowflake/odbc/lib/simba.snowflake.ini

# snowflake - charset settings
ENV LANG en_US.UTF-8
ENV LC_ALL=C.UTF-8

# create snowsql user
RUN groupadd snowsql -g 501 && useradd snowsql -u 501 -g 501
RUN mkdir -p /home/snowsql && touch /home/snowsql/.profile && chown -R 501:501 /home/snowsql

# install snowsql for user snowsql
USER snowsql:snowsql
ADD snowsql-linux_x86_64.bash /home/snowsql
RUN SNOWSQL_DEST=/home/snowsql/bin SNOWSQL_LOGIN_SHELL=/home/snowsql/.profile bash /home/snowsql/snowsql-linux_x86_64.bash
RUN rm /home/snowsql/snowsql-linux_x86_64.bash
RUN /home/snowsql/bin/snowsql -v 1.1.49

# install snowsql for user root
USER root:root
ADD snowsql-linux_x86_64.bash /usr/bin
RUN SNOWSQL_DEST=/usr/bin SNOWSQL_LOGIN_SHELL=~/.profile bash /usr/bin/snowsql-linux_x86_64.bash
RUN rm /usr/bin/snowsql-linux_x86_64.bash
RUN snowsql -v 1.1.49

# install composer
RUN cd \
  && curl -sS https://getcomposer.org/installer | php \
  && ln -s /root/composer.phar /usr/local/bin/composer

ADD . /code
WORKDIR /code

RUN echo "memory_limit = -1" >> /usr/local/etc/php/conf.d/php.ini
RUN echo "date.timezone = \"Europe/Prague\"" >> /usr/local/etc/php/conf.d/php.ini
RUN composer install --no-interaction

CMD php ./run.php --data=/data

