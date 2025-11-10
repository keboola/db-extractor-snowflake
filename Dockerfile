FROM php:8.2-cli-trixie

ARG SNOWFLAKE_ODBC_VERSION=3.10.0
ARG SNOWFLAKE_SNOWSQL_VERSION=1.4.0
ARG SNOWFLAKE_ODBC_GPG_KEY=5A125630709DD64B
ARG SNOWFLAKE_SNOWSQL_GPG_KEY=2A3149C82551A34A
ENV COMPOSER_ALLOW_SUPERUSER=1
ENV DEBIAN_FRONTEND noninteractive
ENV TMPDIR=/opt/snowsqltempdir

RUN mkdir -p /opt/snowsqltempdir

# Install Dependencies
RUN apt-get update \
  && apt-get install unzip git unixodbc unixodbc-dev libpq-dev debsig-verify libicu-dev -y

RUN docker-php-ext-configure intl \
    && docker-php-ext-install intl

# Install PHP odbc extension
# https://github.com/docker-library/php/issues/103
RUN set -x \
    && docker-php-source extract \
    && cd /usr/src/php/ext/odbc \
    && phpize \
    && sed -ri 's@^ *test +"\$PHP_.*" *= *"no" *&& *PHP_.*=yes *$@#&@g' configure \
    && ./configure --with-unixODBC=shared,/usr \
    && docker-php-ext-install odbc \
    && docker-php-source delete

#snoflake download + verify package
COPY driver/snowflake-odbc-policy.pol /etc/debsig/policies/$SNOWFLAKE_ODBC_GPG_KEY/generic.pol
COPY driver/snowflake-snowsql-policy.pol /etc/debsig/policies/$SNOWFLAKE_SNOWSQL_GPG_KEY/generic.pol
COPY driver/simba.snowflake.ini /usr/lib/snowflake/odbc/lib/simba.snowflake.ini
ADD https://sfc-repo.azure.snowflakecomputing.com/odbc/linux/$SNOWFLAKE_ODBC_VERSION/snowflake-odbc-$SNOWFLAKE_ODBC_VERSION.x86_64.deb /tmp/snowflake-odbc.deb
ADD https://sfc-repo.azure.snowflakecomputing.com/snowsql/bootstrap/1.4/linux_x86_64/snowsql-$SNOWFLAKE_SNOWSQL_VERSION-linux_x86_64.bash /usr/bin/snowsql-linux_x86_64.bash
ADD https://sfc-repo.azure.snowflakecomputing.com/snowsql/bootstrap/1.4/linux_x86_64/snowsql-$SNOWFLAKE_SNOWSQL_VERSION-linux_x86_64.bash.sig /tmp/snowsql-linux_x86_64.bash.sig

# snowflake - charset settings
ENV LANG en_US.UTF-8
ENV LC_ALL=C.UTF-8

RUN mkdir -p ~/.gnupg \
    && chmod 700 ~/.gnupg \
    && echo "disable-ipv6" >> ~/.gnupg/dirmngr.conf \
    && mkdir -p /etc/gnupg \
    && echo "allow-weak-digest-algos" >> /etc/gnupg/gpg.conf \
    && mkdir -p /usr/share/debsig/keyrings/$SNOWFLAKE_ODBC_GPG_KEY \
    && mkdir -p /usr/share/debsig/keyrings/$SNOWFLAKE_SNOWSQL_GPG_KEY \
    && if ! gpg --keyserver hkp://keys.gnupg.net --recv-keys $SNOWFLAKE_ODBC_GPG_KEY; then \
      gpg --keyserver hkp://keyserver.ubuntu.com --recv-keys $SNOWFLAKE_ODBC_GPG_KEY;  \
    fi \
    && if ! gpg --keyserver hkp://keys.gnupg.net --recv-keys $SNOWFLAKE_SNOWSQL_GPG_KEY; then \
      gpg --keyserver hkp://keyserver.ubuntu.com --recv-keys $SNOWFLAKE_SNOWSQL_GPG_KEY;  \
    fi \
    && gpg --export $SNOWFLAKE_ODBC_GPG_KEY > /usr/share/debsig/keyrings/$SNOWFLAKE_ODBC_GPG_KEY/debsig.gpg \
    && gpg --export $SNOWFLAKE_SNOWSQL_GPG_KEY > /usr/share/debsig/keyrings/$SNOWFLAKE_SNOWSQL_GPG_KEY/debsig.gpg \
    && debsig-verify /tmp/snowflake-odbc.deb \
    && gpg --verify /tmp/snowsql-linux_x86_64.bash.sig /usr/bin/snowsql-linux_x86_64.bash \
    && gpg --batch --delete-key --yes $SNOWFLAKE_ODBC_GPG_KEY \
    && gpg --batch --delete-key --yes $SNOWFLAKE_SNOWSQL_GPG_KEY \
    && dpkg -i /tmp/snowflake-odbc.deb \
    && SNOWSQL_DEST=/usr/bin SNOWSQL_LOGIN_SHELL=~/.profile bash /usr/bin/snowsql-linux_x86_64.bash \
    && rm /tmp/snowflake-odbc.deb


RUN snowsql -v $SNOWFLAKE_SNOWSQL_VERSION

# install composer
RUN cd \
  && curl -sS https://getcomposer.org/installer | php \
  && ln -s /root/composer.phar /usr/local/bin/composer

ADD . /code
WORKDIR /code

RUN echo "memory_limit = -1" >> /usr/local/etc/php/conf.d/php.ini
RUN echo "date.timezone = \"Europe/Prague\"" >> /usr/local/etc/php/conf.d/php.ini
RUN composer install --no-interaction

CMD php ./src/run.php

