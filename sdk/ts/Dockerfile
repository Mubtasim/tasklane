# syntax=docker/dockerfile:1
# The compose demo's TypeScript worker (design §21.5). Build context: sdk/ts.
FROM node:26-slim AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN --mount=type=cache,target=/root/.npm npm ci --no-audit --no-fund
COPY . .
RUN npm run build && npm run build:examples

FROM node:26-slim
WORKDIR /app
ENV NODE_ENV=production
# The example's OpenTelemetry packages are devDependencies of the SDK, so
# the whole node_modules comes along.
COPY --from=build /app /app
USER node
ENTRYPOINT ["node", "dist-examples/demo-worker.js"]
