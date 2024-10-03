#!/bin/zsh

SF=1
if [ $# -eq 1 ]; then
  SF=$1
fi

echo "=*=*=*=*= Start Linear Road data generation =*=*=*=*="

echo "*** Downlowd files from original GeneaLog repository ***"
echo "(wget -O h1.csv https://chalmersuniversity.box.com/shared/static/ioal17insfry4naurtybkp44dxev59ta.txt)"
wget -O h1.csv https://chalmersuniversity.box.com/shared/static/ioal17insfry4naurtybkp44dxev59ta.txt

echo "*** Increase data volume ***"
echo "(python increaseDataVol.py ${SF})"
python increaseDataVol.py ${SF}

echo "(python checkDuplicate.py)"
python checkDuplicate.py

echo "=*=*=*=*= End Linear Road data generation =*=*=*=*="