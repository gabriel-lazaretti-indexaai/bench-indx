FROM golang:1.27-alpine AS build

WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN mkdir -p /out && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/benchmark .

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=build /out/benchmark /usr/local/bin/benchmark
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/benchmark"]
