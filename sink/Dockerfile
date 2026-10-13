FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/sink . \
 && CGO_ENABLED=0 go build -o /out/topicdump ./cmd/topicdump

FROM alpine:3.22
COPY --from=build /out/sink /usr/local/bin/sink
COPY --from=build /out/topicdump /usr/local/bin/topicdump
ENTRYPOINT ["/usr/local/bin/sink"]
