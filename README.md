# Elasticsearch STAC

Import and fix STAC files into elastic search


## Setup

Create a `.env` file from the `.env.example` and fill in the settings

```bash
cp .env.example .env
```

## Running

```bash
# Install deps
yarn

# compile the code
yarn build

# Import the data
node build/src/index.js
```