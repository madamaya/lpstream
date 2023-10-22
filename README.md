# L3Stream

### How to run
1. Change config files (`./bin/config.sh`, `./flink-conf.yaml`)
2. Run redis `redis-server`
3. Execute the following commands
```
./setup.sh downloads && ./setup.sh compile && ./setup.sh mainData && \
./setup.sh testData && ./setup.sh setup && ./setup.sh test
```
