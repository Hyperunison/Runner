# Running container
```shell
  export UNISON_API_TOKEN=your-token
  export UNISON_DSN="postgresql+psycopg2://username:password@hostname:5433/db"
  curl https://github.com/Hyperunison/Runner/blob/main/docker-compose.local.yml | docker-compose -f docker-compose.local.yaml up
```

# Run container with one command
```shell
    export UNISON_API_TOKEN=your-token; export UNISON_DSN="postgresql+psycopg2://username:password@hostname:5433/db"; curl https://github.com/Hyperunison/Runner/blob/main/docker-compose.local.yml | docker-compose -f docker-compose.local.yaml up
```