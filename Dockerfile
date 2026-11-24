# Build a static binary, run it on distroless as non-root.
FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ cmd/
COPY internal/ internal/
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/pipeline ./cmd/pipeline \
    && mkdir -p /out/data

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build --chown=nonroot:nonroot /out/pipeline /app/pipeline
# distroless has no shell to mkdir at runtime: ship the data directory owned by nonroot.
COPY --from=build --chown=nonroot:nonroot /out/data /app/data
ENTRYPOINT ["/app/pipeline"]
CMD ["run"]
