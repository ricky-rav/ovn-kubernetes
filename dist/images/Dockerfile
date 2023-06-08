FROM rockylinux/rockylinux:8.5 as ovn

USER root

ENV PYTHONDONTWRITEBYTECODE yes

# Get openvswitch from Nvidia controlled source
# Based on:OVS=2.17.1.ea16fb08.fc452de4 OVN=22.03.0.71790c50.fc452de4
# Based on:OVS=2.17.1.ea16fb08.fc452de4 OVN=22.03.0.71790c50.fc452de4

ARG K8S_VER
ARG OVS_VER
ARG OVN_VER

ARG ARTIFACTORY_CERT

# firewall-cmd wrappers
COPY ./iptables-scripts/firewall-cmd /usr/bin/
COPY kubernetes.repo /etc/yum.repos.d/kubernetes.repo
# install epel.repo
RUN yum install -y epel-release
RUN echo -e "\
[ovn-all-Artifactory] \n\
name=ovn-all-Artifactory \n\
baseurl=https://$ARTIFACTORY_CERT@urm.nvidia.com/artifactory/sw-ngn-sdn-rpm/ovn/ovn-all/8/x86_64/ \n\
enabled=1 \n\
gpgcheck=0" > /etc/yum.repos.d/ovn.repo &&\
echo -e "\
[ovs-all-Artifactory] \n\
name=ovs-all-Artifactory \n\
baseurl=https://$ARTIFACTORY_CERT@urm.nvidia.com/artifactory/sw-ngn-sdn-rpm/ovs/ovs-all/8/x86_64/ \n\
enabled=1 \n\
gpgcheck=0" > /etc/yum.repos.d/ovs.repo &&\
INSTALL_PKGS=" \
        bind-utils procps-ng openssl numactl-libs \
        libpcap iproute iputils strace socat unbound-libs python2 nmap\
        " && \
        yum install -y --setopt=tsflags=nodocs --setopt=skip_missing_names_on_install=False $INSTALL_PKGS && \
    yum -y --setopt=tsflags=nodocs --setopt=skip_missing_names_on_install=False update && \
    yum install -y --setopt=tsflags=nodocs --setopt=skip_missing_names_on_install=False \
        kubectl$K8S_VER openvswitch$OVS_VER openvswitch-devel$OVS_VER ovn$OVN_VER ovn-host$OVN_VER ovn-central$OVN_VER && \
    yum autoremove -y && \
    yum clean all && rm -rf /var/cache/dnf/* && rm -f /etc/yum.repos.d/ovn.repo /etc/yum.repos.d/ovs.repo

# ovnkube.sh is the entry point. This script examines environment
# variables to direct operation and configure ovn
COPY ovnkube.sh ovndb-raft-functions.sh ovnkube-node-probe.sh git_info /root/
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
COPY ovnkube ovndbchecker /usr/bin/
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

