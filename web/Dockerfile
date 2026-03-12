FROM oven/bun:1 AS builder

WORKDIR /app

COPY package.json bun.lockb* ./
RUN bun install --frozen-lockfile

COPY . .

ARG API_URL=http://api:3000
ARG PUBLIC_API_URL=http://localhost:3000
ENV API_URL=$API_URL
ENV PUBLIC_API_URL=$PUBLIC_API_URL

RUN bun run build

FROM oven/bun:1-slim

WORKDIR /app

COPY --from=builder /app/dist ./dist
COPY --from=builder /app/node_modules ./node_modules

ENV HOST=0.0.0.0
ENV PORT=4321

EXPOSE 4321

CMD ["bun", "dist/server/entry.mjs"]
