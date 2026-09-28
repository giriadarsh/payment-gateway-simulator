# One image for every Node service; docker-compose.yml picks the entry point.
FROM node:22-alpine

WORKDIR /app
ENV NODE_ENV=production

COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force

COPY src ./src
COPY public ./public

USER node
CMD ["node", "src/dashboard/index.js"]
