FROM golang:1.23-alpine AS builder

RUN apk add upx

WORKDIR /go/src/github.com/slntopp/nocloud-driver-virtual
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -buildvcs=false .
RUN upx ./nocloud-driver-virtual

FROM scratch
WORKDIR /
COPY --from=builder  /go/src/github.com/slntopp/nocloud-driver-virtual/nocloud-driver-virtual /nocloud-driver-virtual
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

EXPOSE 8080
LABEL org.opencontainers.image.source https://github.com/slntopp/nocloud-driver-virtual
LABEL nocloud.update "true"
LABEL nocloud.driver ""

ENTRYPOINT ["/nocloud-driver-virtual"]
