#!/bin/bash

declare -a CALC_MODELS=("TotalDTCalcModel" "DTOverTheWholeWeek" "IntegratingDTClacModel")
declare -a DISRUPTION_BUDGETS=("40" "15")
declare -a GRACE_PERIODS=("14400" "3600" "1800" "900")
declare -a GRACE_PERIODS=("14400" "3600" "1800" "900")

for CALC_MODEL in "${CALC_MODELS[@]}"
do
  for DISRUPTION_BUDGET in "${DISRUPTION_BUDGETS[@]}"
  do
    for GRACE_PERIOD in "${GRACE_PERIODS[@]}"
    do
       echo "$CALC_MODEL $DISRUPTION_BUDGET $GRACE_PERIOD"

        python3 ./calc_downtime.py --restart-date "2023-10-02 10:00:00,000"   --policy RandomIslandLeastLoadedNewNodePolicy  --dt-calc-model "$CALC_MODEL"   --disruption-budget "$DISRUPTION_BUDGET" --grace-period-sec "$GRACE_PERIOD" &
        python3 ./calc_downtime.py --restart-date "2023-10-02 11:00:00,000"   --policy RandomIslandLeastLoadedNewNodePolicy  --dt-calc-model "$CALC_MODEL"   --disruption-budget "$DISRUPTION_BUDGET" --grace-period-sec "$GRACE_PERIOD" &
        python3 ./calc_downtime.py --restart-date "2023-10-02 12:00:00,000"   --policy RandomIslandLeastLoadedNewNodePolicy  --dt-calc-model "$CALC_MODEL"   --disruption-budget "$DISRUPTION_BUDGET" --grace-period-sec "$GRACE_PERIOD" &
        wait

    done
  done
done
#
#
#python3 ./calc_downtime.py --restart-date "2023-10-02 10:00:00,000"   --policy RandomIslandLeastLoadedNewNodePolicy  --dt-calc-model TotalDTCalcModel   --disruption-budget 40 --grace-period-sec 3600 &
#python3 ./calc_downtime.py --restart-date "2023-10-02 11:00:00,000"   --policy RandomIslandLeastLoadedNewNodePolicy  --dt-calc-model TotalDTCalcModel   --disruption-budget 40 --grace-period-sec 3600 &
#python3 ./calc_downtime.py --restart-date "2023-10-02 12:00:00,000"   --policy RandomIslandLeastLoadedNewNodePolicy  --dt-calc-model TotalDTCalcModel   --disruption-budget 40 --grace-period-sec 3600 &
#wait
#
#python3 ./calc_downtime.py --restart-date "2023-10-02 10:00:00,000"   --policy RandomIslandLeastLoadedNewNodePolicy  --dt-calc-model TotalDTCalcModel   --disruption-budget 40 --grace-period-sec 1800 &
#python3 ./calc_downtime.py --restart-date "2023-10-02 11:00:00,000"   --policy RandomIslandLeastLoadedNewNodePolicy  --dt-calc-model TotalDTCalcModel   --disruption-budget 40 --grace-period-sec 1800 &
#python3 ./calc_downtime.py --restart-date "2023-10-02 12:00:00,000"   --policy RandomIslandLeastLoadedNewNodePolicy  --dt-calc-model TotalDTCalcModel   --disruption-budget 40 --grace-period-sec 1800 &
#wait
#
#python3 ./calc_downtime.py --restart-date "2023-10-02 10:00:00,000"   --policy RandomIslandLeastLoadedNewNodePolicy  --dt-calc-model TotalDTCalcModel   --disruption-budget 40 --grace-period-sec 900 &
#python3 ./calc_downtime.py --restart-date "2023-10-02 11:00:00,000"   --policy RandomIslandLeastLoadedNewNodePolicy  --dt-calc-model TotalDTCalcModel   --disruption-budget 40 --grace-period-sec 900 &
#python3 ./calc_downtime.py --restart-date "2023-10-02 12:00:00,000"   --policy RandomIslandLeastLoadedNewNodePolicy  --dt-calc-model TotalDTCalcModel   --disruption-budget 40 --grace-period-sec 900 &
#wait
#
#
#python3 ./calc_downtime.py --restart-date "2023-10-02 10:00:00,000"   --policy RandomIslandLeastLoadedNewNodePolicy  --dt-calc-model TotalDTCalcModel   --disruption-budget 15 --grace-period-sec 14400 &
#python3 ./calc_downtime.py --restart-date "2023-10-02 11:00:00,000"   --policy RandomIslandLeastLoadedNewNodePolicy  --dt-calc-model TotalDTCalcModel   --disruption-budget 15 --grace-period-sec 14400 &
#python3 ./calc_downtime.py --restart-date "2023-10-02 12:00:00,000"   --policy RandomIslandLeastLoadedNewNodePolicy  --dt-calc-model TotalDTCalcModel   --disruption-budget 15 --grace-period-sec 14400 &
#wait
#
#python3 ./calc_downtime.py --restart-date "2023-10-02 10:00:00,000"   --policy RandomIslandLeastLoadedNewNodePolicy  --dt-calc-model TotalDTCalcModel   --disruption-budget 15 --grace-period-sec 3600 &
#python3 ./calc_downtime.py --restart-date "2023-10-02 11:00:00,000"   --policy RandomIslandLeastLoadedNewNodePolicy  --dt-calc-model TotalDTCalcModel   --disruption-budget 15 --grace-period-sec 3600 &
#python3 ./calc_downtime.py --restart-date "2023-10-02 12:00:00,000"   --policy RandomIslandLeastLoadedNewNodePolicy  --dt-calc-model TotalDTCalcModel   --disruption-budget 15 --grace-period-sec 3600 &
#wait
#
#python3 ./calc_downtime.py --restart-date "2023-10-02 10:00:00,000"   --policy RandomIslandLeastLoadedNewNodePolicy  --dt-calc-model TotalDTCalcModel   --disruption-budget 15 --grace-period-sec 1800 &
#python3 ./calc_downtime.py --restart-date "2023-10-02 11:00:00,000"   --policy RandomIslandLeastLoadedNewNodePolicy  --dt-calc-model TotalDTCalcModel   --disruption-budget 15 --grace-period-sec 1800 &
#python3 ./calc_downtime.py --restart-date "2023-10-02 12:00:00,000"   --policy RandomIslandLeastLoadedNewNodePolicy  --dt-calc-model TotalDTCalcModel   --disruption-budget 15 --grace-period-sec 1800 &
#wait
#
#python3 ./calc_downtime.py --restart-date "2023-10-02 10:00:00,000"   --policy RandomIslandLeastLoadedNewNodePolicy  --dt-calc-model TotalDTCalcModel   --disruption-budget 15 --grace-period-sec 900 &
#python3 ./calc_downtime.py --restart-date "2023-10-02 11:00:00,000"   --policy RandomIslandLeastLoadedNewNodePolicy  --dt-calc-model TotalDTCalcModel   --disruption-budget 15 --grace-period-sec 900 &
#python3 ./calc_downtime.py --restart-date "2023-10-02 12:00:00,000"   --policy RandomIslandLeastLoadedNewNodePolicy  --dt-calc-model TotalDTCalcModel   --disruption-budget 15 --grace-period-sec 900 &
#wait
#

#python3 ./calc_downtime.py --restart-date "2023-10-02 10:00:00,000"   --policy RandomIslandLeastLoadedNewNodePolicy  --dt-calc-model IntegratingDTClacModel   --disruption-budget 40 --grace-period-sec 1800 &
#python3 ./calc_downtime.py --restart-date "2023-10-02 11:00:00,000"   --policy RandomIslandLeastLoadedNewNodePolicy  --dt-calc-model IntegratingDTClacModel   --disruption-budget 40 --grace-period-sec 1800 &
#python3 ./calc_downtime.py --restart-date "2023-10-02 12:00:00,000"   --policy RandomIslandLeastLoadedNewNodePolicy  --dt-calc-model IntegratingDTClacModel   --disruption-budget 40 --grace-period-sec 1800 &
#wait

#python3 ./calc_downtime.py --restart-date "2023-10-02 10:00:00,000"   --policy RandomIslandLeastLoadedNewNodePolicy  --dt-calc-model IntegratingDTClacModel   --disruption-budget 40 --grace-period-sec 1800 &
#python3 ./calc_downtime.py --restart-date "2023-10-02 11:00:00,000"   --policy RandomIslandLeastLoadedNewNodePolicy  --dt-calc-model IntegratingDTClacModel   --disruption-budget 40 --grace-period-sec 1800 &
#python3 ./calc_downtime.py --restart-date "2023-10-02 12:00:00,000"   --policy RandomIslandLeastLoadedNewNodePolicy  --dt-calc-model IntegratingDTClacModel   --disruption-budget 40 --grace-period-sec 1800 &
#wait
#
#python3 ./calc_downtime.py --restart-date "2023-10-02 10:00:00,000"   --policy RandomIslandLeastLoadedNewNodePolicy  --dt-calc-model IntegratingDTClacModel   --disruption-budget 40 --grace-period-sec 900 &
#python3 ./calc_downtime.py --restart-date "2023-10-02 11:00:00,000"   --policy RandomIslandLeastLoadedNewNodePolicy  --dt-calc-model IntegratingDTClacModel   --disruption-budget 40 --grace-period-sec 900 &
#python3 ./calc_downtime.py --restart-date "2023-10-02 12:00:00,000"   --policy RandomIslandLeastLoadedNewNodePolicy  --dt-calc-model IntegratingDTClacModel   --disruption-budget 40 --grace-period-sec 900 &
#wait
#
