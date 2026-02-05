# Minimal image for storing build artifacts
# Used to transfer build outputs between CI jobs via Docker registry
FROM busybox:latest
ARG ARTIFACT_DIR=/artifacts
WORKDIR ${ARTIFACT_DIR}
# Files are copied in via build context
COPY . .
