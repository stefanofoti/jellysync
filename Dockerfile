FROM node:22-alpine AS webbuild
WORKDIR /web
COPY web/package.json web/package-lock.json* ./
RUN npm install
COPY web/ .
RUN npm run build

FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ cmd/
COPY internal/ internal/
COPY --from=webbuild /internal/webui/static ./internal/webui/static
RUN go build -o /jellysync ./cmd/jellysync

FROM alpine:3.20
COPY --from=build /jellysync /jellysync
ENTRYPOINT ["/jellysync"]
