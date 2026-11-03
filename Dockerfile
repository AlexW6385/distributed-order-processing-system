# One image with every binary; each Deployment or Compose service picks one
# with its command.
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN for s in api-gateway order-service product-service payment-service notification-service reconciler; do \
      CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/$s ./services/$s/cmd || exit 1; \
    done

FROM alpine:3.20
RUN adduser -D app
USER app
COPY --from=build /out/ /usr/local/bin/
