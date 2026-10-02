FROM node:20-alpine
WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev
COPY server.js ./
COPY proxies.txt ./proxies.txt
COPY .env.example ./.env.example
EXPOSE 3000
CMD ["node", "server.js"]
