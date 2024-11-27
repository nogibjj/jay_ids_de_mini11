install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt

test:
	python -m pytest -vv -cov=mylib test_*.py

format:	
	black *.py 

lint:
	ruff check *.py mylib/*.py
		
all: install lint test format deploy
