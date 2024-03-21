# Technical assignment

## Requirements
```
docker
```

The project is developed and tested using docker so no more local dependencies are required.
 
## Instructions
To start work we need to build the images containing Java, Hadoop and Spark using the command: 
```
make build_images
```

Once the docker images are built we can spin up a dev container using the [docker-compose file](tools/docker/docker_compose/docker-compose-dev.yml)
using the command in a new terminal:

```
make dev_spin_up
``` 

It should be noted that the ports 4040, 8889 on the local machine will be used by the container for exposing 
the Spark UI and for working with Jupyter notebooks.   

To run the application code to create the data, open a new terminal and run. 

```
make run_app
```

This will write the files to `./tests/resources/data/output`. 
The output and solutions can be viewed in the notebook `./tools/notebooks/solutions.ipynb` that can be opened by running 

```
make dev_notebook
```

which can accessed on `localhost:8889` using the link (with the token) provided in the logs after running the command above.
