#!/bin/bash

docker-compose down

rm -rf data log

docker-compose up -d

if ! `grep -q pd0 /etc/hosts`; then echo '127.0.0.1 pd0' >> /etc/hosts; fi
if ! `grep -q tikv0 /etc/hosts`; then echo '127.0.0.1 tikv0' >> /etc/hosts; fi
if ! `grep -q tidb0 /etc/hosts`; then echo '127.0.0.1 tidb0' >> /etc/hosts; fi
if ! `grep -q tics0 /etc/hosts`; then echo '127.0.0.1 tics0' >> /etc/hosts; fi

mvn -f ../../computing/chspark/pom.xml test

docker-compose down
