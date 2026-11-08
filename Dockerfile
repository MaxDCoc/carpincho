# syntax=docker/dockerfile:1

# ---- build stage: compile a static binary ----------------------------------
FROM golang:1.26-alpine AS build
WORKDIR /src
# Copy the module files first so the dependency layer is cached between builds.
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/carpincho ./cmd/carpincho \
 && mkdir /out/data

# ---- runtime stage: nothing but the binary ---------------------------------
# distroless/static has no shell and no package manager, and runs as an
# unprivileged user (uid 65532).
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/carpincho /carpincho
COPY --from=build --chown=65532:65532 /out/data /data
VOLUME /data
EXPOSE 6380
ENTRYPOINT ["/carpincho"]
# Inside a container the server must listen on all interfaces.
CMD ["--bind", "0.0.0.0", "--port", "6380", "--dir", "/data", "--appendfsync", "everysec"]
