# The collector image has no shell, so a static busybox is added for the
# Docker healthcheck (wget against the health_check extension on :13133).
ARG OTEL_COLLECTOR_VERSION=0.149.0
ARG BUSYBOX_VERSION=1.37.0-uclibc

FROM busybox:${BUSYBOX_VERSION} AS tools

FROM otel/opentelemetry-collector-contrib:${OTEL_COLLECTOR_VERSION}
COPY --from=tools /bin/busybox /busybox
