From centos:centos8

RUN yum install python2 net-tools mysql wget nc java -y \
    && yum install fio perf -y \
    && yum clean all \
    && ln -s /usr/bin/python2 /usr/bin/python
