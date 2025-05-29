FROM rockylinux/rockylinux:8.5 as ovn

USER root

ENV PYTHONDONTWRITEBYTECODE yes

# Get openvswitch from Nvidia controlled source
# Based on:OVS_VER=3.5.0.78691c73.28267085 OVN_VER=25.03.0.ec7f28df.29152512
# Based on:OVS_VER=3.5.0.78691c73.28267085 OVN_VER=25.03.0.ec7f28df.29152512

ARG K8S_VER

# TODO: Below we have code that removes '-' prefix if specified:
# OVN_VER=${OVN_VER#-}
# OVS_VER=${OVS_VER#-}
# This code can be removed after CI/CD pipelines will be updated that adds this prefix
ARG OVS_VER  # Example: 3.3.1.5f875996.19422428
ARG OVN_VER  # Example: 24.03.2.e9db0dbb.21320785
ARG ARCH

ARG ARTIFACTORY_CERT

# firewall-cmd wrappers
COPY ./iptables-scripts/firewall-cmd /usr/bin/
# install epel.repo
RUN yum install -y epel-release net-tools

# Note: to update kubernetes version you need to update kubectl.sha256.arm64 & kubectl.sha256.amd64 files:
# $ export K8S_VER={new-version}
# $ curl -L https://storage.googleapis.com/kubernetes-release/release/$K8S_VER/bin/linux/amd64/kubectl.sha256 > ./kubectl.sha256.amd64
# $ curl -L https://storage.googleapis.com/kubernetes-release/release/$K8S_VER/bin/linux/arm64/kubectl.sha256 > ./kubectl.sha256.arm64
# See SDN-3261 for more details.
COPY kubectl.sha256.$ARCH /tmp/
RUN curl -L https://storage.googleapis.com/kubernetes-release/release/v1.29.8/bin/linux/$ARCH/kubectl > /usr/bin/kubectl \
   && echo `cat /tmp/kubectl.sha256.$ARCH` /usr/bin/kubectl | sha256sum -c - \
   && chmod +x /usr/bin/kubectl

RUN echo -e "\
[ovn-all-Artifactory] \n\
name=ovn-all-Artifactory \n\
baseurl=https://$ARTIFACTORY_CERT@urm.nvidia.com/artifactory/sw-ngn-sdn-rpm/ovn/ovn-all/8/\$basearch/ \n\
enabled=1 \n\
gpgcheck=0" > /etc/yum.repos.d/ovn.repo &&\
echo -e "\
[ovs-all-Artifactory] \n\
name=ovs-all-Artifactory \n\
baseurl=https://$ARTIFACTORY_CERT@urm.nvidia.com/artifactory/sw-ngn-sdn-rpm/ovs/ovs-all/8/\$basearch/ \n\
enabled=1 \n\
gpgcheck=0" > /etc/yum.repos.d/ovs.repo &&\
OVN_VER=${OVN_VER#-} &&\
OVS_VER=${OVS_VER#-} &&\
INSTALL_PKGS=" \
        bind-utils procps-ng openssl numactl-libs \
        libpcap iproute iputils strace socat unbound-libs python2 nmap nftables\
        " && \
        yum install -y --setopt=tsflags=nodocs --setopt=skip_missing_names_on_install=False $INSTALL_PKGS && \
    yum -y --setopt=tsflags=nodocs --setopt=skip_missing_names_on_install=False update && \
    yum install -y --setopt=tsflags=nodocs --setopt=skip_missing_names_on_install=False \
        python3-openvswitch-$OVS_VER-1.el8.noarch openvswitch-$OVS_VER openvswitch-devel-$OVS_VER ovn-$OVN_VER ovn-host-$OVN_VER ovn-central-$OVN_VER && \
    yum autoremove -y && \
    yum clean all && rm -rf /var/cache/dnf/* && rm -f /etc/yum.repos.d/ovn.repo /etc/yum.repos.d/ovs.repo

# ovnkube.sh is the entry point. This script examines environment
# variables to direct operation and configure ovn
COPY ovnkube.sh ovndb-raft-functions.sh ovnkube-node-readiness-check.sh ovnkube-node-liveness-check.sh git_info /root/
COPY ovn-kube-util /usr/bin/

LABEL io.k8s.display-name="ovn" \
      io.k8s.description="ovn centos image" \
      maintainer="Girish Moodalbail <gmoodalbail@nvidia.com>"

WORKDIR /root
ENTRYPOINT /root/ovnkube.sh

FROM ovn as ovnkube

RUN  mkdir -p /usr/libexec/cni/

# Built in ../../go_controller, then the binaries are copied here.
# put things where they are in the rpm
COPY ovnkube ovndbchecker ovnkube-identity /usr/bin/
COPY ovn-k8s-cni-overlay /usr/libexec/cni/ovn-k8s-cni-overlay

COPY git_info /root/
# override the rpm's ovn_k8s.conf with this local copy
COPY ovn_k8s.conf /etc/openvswitch/ovn_k8s.conf

# iptables wrappers
COPY ./iptables-scripts/ip* /usr/sbin/

LABEL io.k8s.display-name="ovn-kubernetes" \
      io.k8s.description="ovnkube centos image" \
      maintainer="Girish Moodalbail <gmoodalbail@nvidia.com>"

WORKDIR /root
ENTRYPOINT /root/ovnkube.sh

