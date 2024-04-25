#===============
# Stage 1: Build
#===============

FROM golang:1.22-alpine as builder

COPY . /app

WORKDIR /app

#RUN go build -o enginetwo
RUN apk add --no-cache gcc g++ git openssh-client
RUN CGO_ENABLED=1 go build -ldflags="-w -s" -o enginetwo
#===============
# Stage 2: Run
#===============

FROM golang:1.22-alpine

COPY --from=builder /app/enginetwo /usr/local/bin/enginetwo

ENTRYPOINT [ "/usr/local/bin/enginetwo" ]