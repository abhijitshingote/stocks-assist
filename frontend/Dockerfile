# build stage
FROM node:18-alpine as build
WORKDIR /app
COPY package.json vite.config.js ./
COPY public ./public
COPY src ./src
RUN npm ci || npm i
RUN npm run build

# serve stage
FROM nginx:alpine
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]

