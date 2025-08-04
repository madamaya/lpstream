# L3Stream

## How to run the experiments
Basically, please refer to version `6822e0d`.
For ordinary# experiments only, please use the latest version.

### Dependencies
The following Python packages and commands are required.
- Python packages
  - tqdm
  - kafka-python
  - pyarrow
  - numpy
  - matplotlib
  - redis
  - pandas
  - scipy
- Commands
  - lein
  - jq
  - python
  - mvn
  - zsh
  - awk (GNU awk is required.)

### Experiments execution
1. Enter your configurations in config files (`./bin/config.sh`, `./flink-conf.yaml`).
2. Run redis `redis-server`
3. Execute the following commands to compile lpstream, download flink and kafka, and generate input datasets.
```
./setup.sh compile && ./setup.sh downloads && ./setup.sh mainData
```
Note that the experiments require much storage.
It is recommended that the above commands be executed on the machine with > 150 GB disk space.

4. Set up the enviroment with the command.
```
./setup.sh setup
```
5. Run experiments.
```
./evaluate.sh latency && ./evaluate.sh throughput && ./evaluate.sh duration
```
Running experiments for all workflows takes too much time.
You can select workflows to evaluate performance by editing the following scripts. 
- `./bin/evaluate/latency.sh` (for latency experiments)
- `./bin/evaluate/throughput.sh` (for throughput experiments)
- `./bin/getLineage/lineageDuration.sh` (for duration experiments).

### Prototype implementation
#### Dependency
The prototype system also uses a repository [[here](https://github.com/madamaya/lpstream-genealog)] as an external library.
It contains GeneaLog programs extended for our system.
`./setup.sh compile` listed above clones and compiles it automatically, so you do not care anything.

#### Workflow programs
Programs for experimental workflows are placed at `./src/main/java/com/madamaya/l3stream/workflows`.
When the workflow name is xxx, programs for the workflow are placed at `./src/main/java/com/madamaya/l3stream/workflows/xxx`.
In the directory, xxx.java corresponds to the original workflow, and GLxxx.java does to GeneaLog.
L3xxx.java is for the ordinary workflow and the provenance workflow, which can be switched via an argument for the program.

#### Simplification of the preprocessor
Currently, LPStream prepares the ordinary workflow and the provenance workflow manually in advance, and the user side sends them instead of the Workflow Preprocessor.
The preprocessing procedure basically follows the algorithm in the paper.
However, the simplification is just for ease of implementation.
If a correct preprocessor is implemented, the original workflow can be preprocessed automatically.