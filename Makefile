VENV=. venv/bin/activate

venv:
	python3.8 -m venv venv
	${VENV} ; pip install -r requirements.txt


format:
	isort *.py
	black *.py

test:
	pytest -W ignore::pytest.PytestCollectionWarning

config:
	python kraft.py config

start:
	python kraft.py start

start0:
	python kraft.py start --start-only 0

start1:
	python kraft.py start --start-only 1

client:
	python kraft.py client
