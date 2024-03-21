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
- assumes each file contains data complete for one day (hence confidently overwriting instead of merging). at least assumes if date is present it has all data for that day.
  => this allows for backfills (assume corrected files are overwritten, else will keep data in raw for both)

#### Silver

##### Deduplication
Data is deduplicated if there are more than one measurement for a turbine at a given time. 
One of the rows are chosen arbitrarily, due to the lack of fields to further distinguish duplicate measurements.  

##### Missing power output values
Having investigated the power output values in [this](./tools/notebooks/EDA.ipynb) notebook, there does not seem to a 'seasonal' pattern 
across days or within days. Also, there's no indication that the wind speed or wind direction would help in predicting the power output.
Hence, a simple average or linear interpolation would be potential options, where the imputed value using linear interpolation would be the 
average of the two adjacent measurements. However, the power output values does not seem to depend on the adjacent measurement values so linear interpolation 
is probably not suitable.
This leaves us with the option of using a simple average within a given day.

##### Outliers
