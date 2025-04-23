FROM rockylinux/rockylinux:8.8

ARG go_version=1.23.4
ARG go_sha256=6924efde5de86fe277676e929dc9917d466efa02fb934197bc2eba35d5680971
ARG go_platform=linux-amd64
ARG go_prefix=/usr/local
ARG ginkgo_version

RUN echo "Run for target: ${TARGETPLATFORM}" \
   && dnf install -y epel-release \
   && dnf config-manager --add-repo=https://download.docker.com/linux/centos/docker-ce.repo

RUN dnf -y install \
      pkgconfig \
      rpm-build \
      make \
      git \
      gcc \
      sudo \
      iptables \
      procps
# gcc, sudo, procps are needed for make check

RUN curl -L https://go.dev/dl/go${go_version}.${go_platform}.tar.gz > /tmp/go.tar.gz \
   && echo ${go_sha256} /tmp/go.tar.gz | sha256sum -c - \
   && tar -C ${go_prefix} -xzf /tmp/go.tar.gz \
   && rm /tmp/go.tar.gz \
   && GOPATH=${go_prefix}/go ${go_prefix}/go/bin/go install github.com/onsi/ginkgo/v2/ginkgo@${ginkgo_version}
ENV PATH=${go_prefix}/go/bin:${PATH}
ENV GOROOT=${go_prefix}/go
