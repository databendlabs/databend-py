prepare:
	mkdir -p data/databend

up: prepare
	docker compose up --quiet-pull -d databend --wait
	curl  -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d '{"sql": "select version()",  "pagination": { "wait_time_secs": 10}}'

start: up

test:
	uv run pytest .

ci:
	uv run pytest .

lint:
	uv run ruff check

