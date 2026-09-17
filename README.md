# demo1

Django + PostGIS + Prefect, orchestrated with Docker Compose.

## Services

- **db** — Postgres 16 with PostGIS 3.4. Also hosts a second database (`prefect`) for the Prefect API backend, created automatically on first boot.
- **web** — Django app (GeoDjango enabled) served on [http://localhost:8000](http://localhost:8000).
- **prefect-server** — Prefect API/UI on [http://localhost:4200](http://localhost:4200), backed by Postgres.
- **prefect-worker** — Process worker polling the `default-pool` work pool, with `prefect/flows` mounted in.

## Getting started

```bash
cp .env.example .env
docker compose up --build
```

Then, in another terminal, run migrations:

```bash
docker compose exec web python manage.py migrate
docker compose exec web python manage.py createsuperuser
```

Django admin: http://localhost:8000/admin/
Prefect UI: http://localhost:4200

## Running the example flow

```bash
docker compose exec prefect-worker python flows/example_flow.py
```

To deploy it to the worker's pool instead of running it directly, use `prefect deploy` from within the `prefect-worker` container once you've defined a `prefect.yaml`.
