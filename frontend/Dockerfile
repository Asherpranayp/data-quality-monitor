FROM node:20-alpine

WORKDIR /app

# Copy package files
COPY package.json yarn.lock ./

# Install dependencies
RUN yarn install --frozen-lockfile

# Copy application code
COPY . .

# Build for production
RUN yarn build

# Install serve for production
RUN yarn global add serve

# Expose port
EXPOSE 3000

# Serve the production build
CMD ["serve", "-s", "build", "-l", "3000"]
