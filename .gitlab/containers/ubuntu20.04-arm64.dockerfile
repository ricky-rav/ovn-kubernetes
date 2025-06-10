FROM ubuntu:20.04
ARG DEBIAN_FRONTEND=noninteractive

ARG go_version=1.23.4
ARG go_sha256=16e5017863a7f6071363782b1b8042eb12c6ca4f4cd71528b2123f0a1275b13e
ARG go_platform=linux-arm64
ARG go_prefix=/usr/local

RUN apt-get update \
   && apt-get install -y \
         debhelper \
         curl \
         make \
         git

RUN curl -L https://go.dev/dl/go${go_version}.${go_platform}.tar.gz > /tmp/go.tar.gz \
   && echo ${go_sha256} /tmp/go.tar.gz | sha256sum -c - \
   && tar -C ${go_prefix} -xzf /tmp/go.tar.gz \
   && rm /tmp/go.tar.gz
ENV PATH=${go_prefix}/go/bin:${PATH}
