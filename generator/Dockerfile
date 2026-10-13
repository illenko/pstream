FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/generator .

FROM alpine:3.22
COPY --from=build /out/generator /usr/local/bin/generator
ENTRYPOINT ["/usr/local/bin/generator"]
