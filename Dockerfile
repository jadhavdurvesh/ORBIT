FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/orbitd ./cmd/orbitd
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/orbit-worker ./cmd/orbit-worker

FROM alpine:3.20
RUN addgroup -S orbit && adduser -S -G orbit orbit
WORKDIR /app
COPY --from=build /out/orbitd /usr/local/bin/orbitd
COPY --from=build /out/orbit-worker /usr/local/bin/orbit-worker
RUN mkdir -p /data && chown -R orbit:orbit /data
USER orbit
ENV ORBIT_ADDR=:8080
ENV ORBIT_DATA_FILE=/data/orbit.json
EXPOSE 8080
ENTRYPOINT ["orbitd"]
