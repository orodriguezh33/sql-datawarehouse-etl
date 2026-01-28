# ------------------------------------------------------------------------------
# Base image
# ------------------------------------------------------------------------------
# Use a lightweight Python image based on Debian Bookworm.
# This keeps the image small while remaining compatible with Microsoft packages.
FROM python:3.12-slim-bookworm


# ------------------------------------------------------------------------------
# Environment configuration
# ------------------------------------------------------------------------------
# PYTHONDONTWRITEBYTECODE:
#   Prevents Python from writing .pyc files (cleaner containers).
#
# PYTHONUNBUFFERED:
#   Ensures logs are streamed directly to stdout/stderr (important for Docker logs).
#
# DEBIAN_FRONTEND:
#   Avoids interactive prompts during apt installs.
#
# ACCEPT_EULA:
#   Required to install Microsoft SQL Server tools non-interactively.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive \
    ACCEPT_EULA=Y


# ------------------------------------------------------------------------------
# Working directory
# ------------------------------------------------------------------------------
# All application code will live under /app
WORKDIR /app


# ------------------------------------------------------------------------------
# System dependencies + SQL Server tools
# ------------------------------------------------------------------------------
# This block installs:
#   - System utilities required for package installation
#   - Microsoft repository signing key
#   - SQL Server command-line tools (sqlcmd)
#   - ODBC Driver 18 for SQL Server
#
# These tools allow the ETL container to:
#   - Connect to SQL Server
#   - Execute .sql scripts
#   - Run stored procedures as part of the pipeline
RUN apt-get update && apt-get install -y --no-install-recommends \
      curl \
      gnupg \
      ca-certificates \
      apt-transport-https \
      unixodbc \
  && curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor \
      > /usr/share/keyrings/microsoft.gpg \
  && echo "deb [signed-by=/usr/share/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
      > /etc/apt/sources.list.d/microsoft-prod.list \
  && apt-get update \
  && ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
      mssql-tools18 \
      msodbcsql18 \
  && rm -rf /var/lib/apt/lists/*


# ------------------------------------------------------------------------------
# Python dependencies
# ------------------------------------------------------------------------------
# Install Python dependencies separately to leverage Docker layer caching.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


# ------------------------------------------------------------------------------
# Application source code
# ------------------------------------------------------------------------------
# Copy the full ETL project into the container.
COPY . .


# ------------------------------------------------------------------------------
# Container entrypoint
# ------------------------------------------------------------------------------
# Execute the ETL pipeline.
# This runs the full Bronze → Silver → Gold process.
CMD ["python", "main.py"]
