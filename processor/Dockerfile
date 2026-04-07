FROM golang:1.26 AS builder

WORKDIR /go/src/app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

# Build
RUN CGO_ENABLED=0 go build -o main ./processor/

FROM alpine:3.23
COPY --from=builder /go/src/app/main ./processor
CMD ["./processor"]
