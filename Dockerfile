FROM node:22-slim

# Create a non-root user
RUN useradd -m -u 1000 user
USER user

WORKDIR /home/user/app

# Install dependencies first for layer caching
COPY --chown=user package*.json ./
RUN npm install --omit=dev

# Copy the worker
COPY --chown=user . .

# Do NOT set ENV PORT here — Suga injects it at runtime.
# Your worker falls back to 8080 if none is provided.
EXPOSE 8080

CMD ["node", "worker.js"]
