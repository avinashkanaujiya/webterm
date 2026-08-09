FROM node:22-slim
RUN apt-get update && apt-get install -y --no-install-recommends python3 make g++ util-linux \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY server.js ./
COPY public ./public
RUN cp node_modules/@xterm/xterm/lib/xterm.js public/ \
    && cp node_modules/@xterm/xterm/css/xterm.css public/ \
    && cp node_modules/@xterm/addon-fit/lib/addon-fit.js public/ \
    && cp node_modules/@xterm/addon-clipboard/lib/addon-clipboard.js public/ \
    && cp node_modules/@xterm/addon-webgl/lib/addon-webgl.js public/ \
    && cp node_modules/@xterm/addon-canvas/lib/addon-canvas.js public/ \
    && cp node_modules/@xterm/addon-unicode11/lib/addon-unicode11.js public/
EXPOSE 7682
CMD ["node", "server.js"]
