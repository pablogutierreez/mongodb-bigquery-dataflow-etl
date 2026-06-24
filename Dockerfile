# Dataflow Flex Template image for the MongoDB → BigQuery ETL.
#
# Two stages:
#   1. Pull the Flex Template launcher binary from Google's base image.
#   2. Build on the matching Beam Python SDK image and copy the launcher in.
#
# The Beam SDK version here MUST match apache-beam in requirements.txt so that
# the workers and the launcher agree on the SDK version.

FROM gcr.io/dataflow-templates-base/python311-template-launcher-base:latest AS template_launcher
FROM apache/beam_python3.11_sdk:2.74.0

WORKDIR /pipeline

# Install dependencies first so this layer is cached across code-only changes.
COPY pipeline/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy in the Flex Template launcher binary.
RUN mkdir -p /opt/google/dataflow
COPY --from=template_launcher /opt/google/dataflow/ /opt/google/dataflow/

# Copy the pipeline source and install local modules (transformations, metrics).
COPY pipeline/ .
RUN pip install --no-cache-dir .

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH="/pipeline"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/pipeline/main.py"
