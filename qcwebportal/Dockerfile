FROM node:23-alpine3.20 AS builder
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm install
COPY . .
RUN npm run devbuild

FROM nginx:stable-alpine AS runner
WORKDIR /app
COPY --from=builder /app/dist /srv
COPY docker/nginx.conf /etc/nginx/conf.d/default.conf
CMD ["nginx", "-g", "daemon off;"]

