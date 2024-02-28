## To launch calculation of downtime
1. place your source calls data to ipnb/calls_data_week.tsv
2. 
```shell
cd rolling_update_downtime/ipnb
python3 ./calc_downtime.py --restart-date "2023-10-02 13:00:00,000" \
  --policy [RandomNewNodePolicy|RoundRobinNewNodePolicy|LeastLoadedNewNodePolicy|RandomIslandLeastLoadedNewNodePolicy]\
  --dt-calc-model [IntegratingDTClacModel|DTOverRolloutPeriod|DTOverTheWholeWeek] \
  --disruption-budget 10
  --grace-period-sec 30
```
3. boot up jupiter notebook
```shell
cd rolling_update_downtime
docker-compose up -d
```
3. pick up the corresponding result_*.tsv and display it with jupiter notebook ipnb/graphs.ipnb