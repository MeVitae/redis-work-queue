# --- Builder --- #
FROM golang:1-bookworm as builder

WORKDIR /usr/src/app

COPY . .
WORKDIR k8s-autoscale/binary
RUN go mod download && go mod verify
RUN go build -o /usr/src/app/mevitae-autoscale .

# --- Image --- #
FROM debian:bookworm-slim
RUN apt update && \
    apt upgrade -y && \
    apt install -y ca-certificates && \
    apt autoremove -y && apt autoclean && \
    rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/src/app/mevitae-autoscale /usr/local/bin/mevitae-autoscale
# The configuration is expected to be mounted from a configmap
CMD ["mevitae-autoscale", "/etc/k8s-wqautoscale/autoscale.yaml"]
