FROM node:22-alpine

WORKDIR /app

COPY . .

# server.js binds 127.0.0.1 by default, which is unreachable from outside the
# container. Set PROXY_HOST=0.0.0.0 in the container's environment (compose
# does this); a non-loopback bind also requires PROXY_ADMIN_TOKEN to be set.

# .env / usage.json / proxy.log live here instead of next to server.js, so
# the docker-compose volume at /app/data persists them across recreates.
ENV PROXY_DATA_DIR=/app/data

EXPOSE 8085

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD wget -qO- http://127.0.0.1:8085/health || exit 1

CMD ["node", "server.js"]
