ARG RUBY_IMAGE=ruby:4.0.2-slim
FROM ${RUBY_IMAGE}

ARG DUCKDB_VERSION=v1.5.2

WORKDIR /app

RUN apt-get update \
  && apt-get install -y --no-install-recommends build-essential ca-certificates curl unzip \
  && rm -rf /var/lib/apt/lists/*

RUN curl -L -o /tmp/libduckdb.zip "https://github.com/duckdb/duckdb/releases/download/${DUCKDB_VERSION}/libduckdb-linux-amd64.zip" \
  && mkdir -p /tmp/libduckdb \
  && unzip -q /tmp/libduckdb.zip -d /tmp/libduckdb \
  && cp /tmp/libduckdb/duckdb.h /tmp/libduckdb/duckdb.hpp /usr/local/include/ \
  && cp /tmp/libduckdb/libduckdb.so /usr/local/lib/ \
  && ldconfig \
  && rm -rf /tmp/libduckdb /tmp/libduckdb.zip

COPY Gemfile Gemfile.lock ./
COPY implementations/gemfiles/ implementations/gemfiles/

RUN bundle install
RUN BUNDLE_GEMFILE=implementations/gemfiles/ruby_csv_default.gemfile bundle install
RUN BUNDLE_GEMFILE=implementations/gemfiles/smarter_csv.gemfile bundle install
RUN BUNDLE_GEMFILE=implementations/gemfiles/polars.gemfile bundle install
RUN BUNDLE_GEMFILE=implementations/gemfiles/duckdb.gemfile bundle config set build.duckdb --with-duckdb-dir=/usr/local \
  && BUNDLE_GEMFILE=implementations/gemfiles/duckdb.gemfile bundle install

COPY . .
