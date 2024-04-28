#===============
# Stage 1: Build
#===============

FROM ubuntu:latest as builder

COPY . /app

WORKDIR /app

RUN apt update && apt install -y sudo wget gcc g++ git
ADD install_go.sh /app
RUN /bin/bash install_go.sh
RUN CGO_ENABLED=1  /usr/local/go/bin/go build -ldflags="-w -s" -o enginetwo

#===============
# Stage 2: Run
#===============

FROM ubuntu:latest

COPY --from=builder /app/enginetwo /usr/local/bin/enginetwo
ADD permissions.sh /opt
RUN apt update && apt install -y  sudo
