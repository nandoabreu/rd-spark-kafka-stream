env-setup:
	python -m venv virtualenv
	bash virtualenv/bin/activate && python -m pip install -r requirements.txt
