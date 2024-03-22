project ?= colibri

check:
	@echo "\n>>> Formatting python code..."
	python3 -m black src

clean:
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/

build: clean
	docker run \
	--rm -v '$(CURDIR)/dist:/dist' \
	--mount type=bind,source='$(CURDIR)/src',target=/src \
	--mount type=bind,source='$(CURDIR)/pyproject.toml',target=/pyproject.toml \
	${project}/poetry bash -c "poetry build"

build_images:
	docker build -t ${project}/spark -f ./tools/docker/dockerfiles/Dockerfile.spark --platform=linux/amd64 .
	docker build -t ${project}/dev -f ./tools/docker/dockerfiles/Dockerfile.dev --platform=linux/amd64 .

dev_spin_up:
	docker-compose \
	-p ${project} \
	-f ./tools/docker/docker_compose/docker-compose-dev.yml \
	up

dev_spin_down:
	docker-compose \
	-p ${project} \
	-f ./tools/docker/docker_compose/docker-compose-dev.yml \
	down

dev_notebook:
	docker exec -it -w /notebooks ${project}_dev jupyter notebook --port=8889 --no-browser --ip=0.0.0.0 --allow-root

test:
	docker exec -it ${project}_dev python3 -m pytest tests/${test_name}

run_app:
	docker exec -it -w /src/${project} ${project}_dev python3 main.py
