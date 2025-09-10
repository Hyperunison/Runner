# Running container
```shell
  curl https://github.com/Hyperunison/Runner/blob/main/docker-compose.local.yml
  echo UNISON_API_TOKEN=your-token > .env
  echo UNISON_DSN="postgresql+psycopg2://username:password@hostname:5433/db" > .env
  docker-compose -f docker-compose.local.yaml up
```