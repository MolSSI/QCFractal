FROM node:22-alpine AS builder
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
# Server address is baked in at build time (default is the same origin as the portal).
# This may also come from a .env.production/.env.local file in the build context.
ARG VITE_QCFRACTAL_URI
RUN npm run devbuild

FROM nginx:stable-alpine AS runner
WORKDIR /app
COPY --from=builder /app/dist /srv
COPY docker/nginx.conf /etc/nginx/conf.d/default.conf
CMD ["nginx", "-g", "daemon off;"]
