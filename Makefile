PY=python3

.PHONY: setup migrate fast deep
setup:
	$(PY) -m venv .venv && . .venv/bin/activate && pip install -U pip && pip install -e .

migrate:
	mc migrate --config /home/dl/development/.config/music-catalog/dev.yaml

fast:
	mc inventory fast --config /home/dl/development/.config/music-catalog/dev.yaml

deep:
	mc inventory deep --config /home/dl/development/.config/music-catalog/dev.yaml
