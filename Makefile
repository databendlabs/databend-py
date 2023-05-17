test:
	python tests/test_client.py

ci:
	python tests/test_client.py

install:
	pip install -r requirements.txt
	pip install -e .
