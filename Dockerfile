# Use official Node.js LTS version
FROM node:14-alpine

# Set working directory
WORKDIR /app

# Install app dependencies
COPY package*.json ./
RUN npm install

# Bundle app source
COPY . .

# Expose port
EXPOSE 5555

# Start app
CMD ["npm", "start"]
