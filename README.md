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

## Assignment
### Data
All turbines are expected to have an hourly measurement.

### Application
#### Bronze
Keep raw data in its rawest for
- infer schema: a) no errors casting potentially losing information b) allows for schema evolution
- no dedupe, can monitor on bronze table

Assumptions:
- assumes each file contains data complete for one day (hence confidently overwriting). at least assumes if date is present it has all data for that day.
  => this allows for backfills (assume corrected files are overwritten, else will keep data in raw for both)

#### Silver
Assumes no dupes, otherwise would have to be deduped based on timestamp, turbine_id (would have to choose one value)
