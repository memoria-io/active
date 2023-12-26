FROM memoriaio/java-docker:21.0.1

MAINTAINER Ismail Marmoush<marmoushismail@gmail.com>

ENV NATS_DEB=https://github.com/nats-io/natscli/releases/download/v0.1.1/nats-0.1.1-amd64.deb

RUN wget $NATS_DEB -O nats.deb
RUN dpkg -i nats.deb
