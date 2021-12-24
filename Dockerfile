# build environment
FROM golang:1.17-alpine as build
RUN mkdir -vp /app
COPY ./go.mod /app/go.mod
COPY ./go.sum /app/go.sum
COPY ./apis /app/apis
COPY ./cmd /app/cmd
COPY ./common /app/common
COPY ./core /app/core
COPY ./dataplane /app/dataplane
COPY ./management /app/management
COPY ./main.go /app/main.go
RUN cd /app && \
    go build -o httpmq.bin . && \
    cp -v ./httpmq.bin /usr/bin/

# production environment
FROM alpine
COPY --from=build /usr/bin/httpmq.bin /usr/bin/
ENTRYPOINT ["/usr/bin/httpmq.bin"]
