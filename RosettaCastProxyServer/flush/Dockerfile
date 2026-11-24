FROM node:20-slim
WORKDIR /app
COPY package.json .
RUN npm install
COPY flushHandler.js .
CMD ["node", "flushHandler.js"]
