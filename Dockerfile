FROM node:24-alpine
WORKDIR /app
COPY server.js ./
CMD ["node", "server.js"]
