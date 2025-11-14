# Adapted from https://github.com/astral-sh/uv-docker-example/blob/main/Dockerfile
FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim

RUN apt-get update && apt-get install -y git

WORKDIR /app

COPY . .

# Install dependencies in the fixed prd group
RUN uv sync --group prd
# Add the virtual environment to the PATH
ENV PATH="/app/.venv/bin:$PATH"
# Override the entrypoint to not be uv
ENTRYPOINT []

CMD ["cas_sdss_mcp"]
