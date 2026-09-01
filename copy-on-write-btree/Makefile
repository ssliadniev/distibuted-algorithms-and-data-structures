.PHONY: test test-docker clean

test:
	PYTHONPATH=$(PWD) pytest tests/ -v

test-docker:
	docker-compose up --build --abort-on-container-exit

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	rm -f *.db tests/*.db
