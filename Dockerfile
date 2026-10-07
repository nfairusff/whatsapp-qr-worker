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

# Suga injects PORT; default to 8080
ENV PORT=8080
EXPOSE 8080

CMD ["node", "worker.js"]
