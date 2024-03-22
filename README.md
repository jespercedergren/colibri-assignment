# Technical assignment

## Requirements
```
docker
```

The project is developed and tested using docker so no more local dependencies are required.
The code can be run locally without docker as well.
 
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

This will write the files to `./src/tests/resources/data/output`.

```
make dev_notebook
```

which can accessed on `localhost:8889` using the link (with the token) provided in the logs after running the command above.

## Assignment
### Data
- All turbines are expected to have an hourly measurement.
- Rows are not expected to be missing.

### Application
#### Bronze
Keep raw data in its rawest form:
- Infer the schema: 
  - a) no errors casting potentially losing information 
  - b) allows for automatic schema evolution
- no dedupe logic, can monitor the Bronze table to detect.

Assumptions:
- Assumes each file contains data complete for one day, hence confidently overwriting instead of merging (at least assumes if date is present it has all data for that day).

This allows for backfills/corrections.

#### Silver

##### Deduplication
Data is deduplicated if there are more than one measurement for a turbine at a given time. 
One of the rows are chosen arbitrarily, due to the lack of fields to further distinguish duplicate rows.  

##### Missing (power output) values
Having investigated the power output values in [this](./tools/notebooks/EDA.ipynb) notebook, there does not seem to a 'seasonal' pattern 
across days or within days. Also, there's no indication that the wind speed or wind direction would help in predicting the power output.
Hence, a simple average or linear interpolation would be potential options, where the imputed value using linear interpolation would be the 
average of the two adjacent measurements. However, the power output values does not seem to depend on the adjacent measurement values so linear interpolation 
is deemed not suitable.
This leaves us with the option of using a simple average within a given day to impute missing values. 
To indicate to users/consumers of the table that the value is imputed the column `flag_imputed_power_output` is set to `true`. 
Alternatively, the original column could have remained untouched and an extra column `imputed_power_output` could have been added. 

##### Outliers (power output)
Having explored the data in the EDA notebook mentions above, no obvious visual outliers are present in the data.
Outliers or anomalies are currently flagged as an anomaly, in the column `flag_imputed_power_output`, if the value is not within 2 standard deviations.
This limit is chosen arbitrarily without any data or domain knowledge, for illustrative purpose only.
In order to not obfuscate the data, the values are not replaced, the user is presented with a flag and handle outliers how they see fit. 
Same as for missing values, `imputed_power_output` could be used to both replace null values and outliers while at the same time keeping the original value.  

##### Missing rows
As mentioned above, rows are not expected to be missing.

### Future improvements
- Use Delta tables.
