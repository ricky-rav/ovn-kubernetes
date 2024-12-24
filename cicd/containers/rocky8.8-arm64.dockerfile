FROM rockylinux/rockylinux:8.8

ARG go_version=1.23.4
ARG go_sha256=16e5017863a7f6071363782b1b8042eb12c6ca4f4cd71528b2123f0a1275b13e
ARG go_platform=linux-arm64
ARG go_prefix=/usr/local

RUN echo "Run for target: ${TARGETPLATFORM}" \
   && dnf install -y epel-release

RUN dnf config-manager --add-repo=https://download.docker.com/linux/centos/docker-ce.repo

RUN dnf -y install \
      pkgconfig \
      rpm-build \
      make \
      git

RUN curl -L https://go.dev/dl/go${go_version}.${go_platform}.tar.gz > /tmp/go.tar.gz \
   && echo ${go_sha256} /tmp/go.tar.gz | sha256sum -c - \
   && tar -C ${go_prefix} -xzf /tmp/go.tar.gz \
   && rm /tmp/go.tar.gz
ENV PATH=${go_prefix}/go/bin:${PATH}
