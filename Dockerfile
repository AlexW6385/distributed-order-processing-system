# One image definition for every service: docker build --build-arg SERVICE=order-service .
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG SERVICE
RUN test -n "$SERVICE" && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /bin/service ./services/${SERVICE}/cmd

FROM alpine:3.20
RUN adduser -D app
USER app
COPY --from=build /bin/service /bin/service
ENTRYPOINT ["/bin/service"]
