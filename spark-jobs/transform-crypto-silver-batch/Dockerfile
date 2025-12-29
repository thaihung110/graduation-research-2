FROM apache/spark:3.5.0-python3

# Switch to root for any setup needed
USER root

# Set working directory
WORKDIR /app

# Copy Python application code (src directory)
COPY src/ /app/src/

# Make script executable and set ownership/permissions
RUN chmod +x /app/src/main.py && \
    chmod 777 /app && \
    chown -R 185:185 /app

# Switch back to default spark user (uid 185)
USER 185

# Set PYTHONPATH to include src directory
ENV PYTHONPATH=/app/src:$PYTHONPATH

# Default command
CMD ["python", "/app/src/main.py"]
