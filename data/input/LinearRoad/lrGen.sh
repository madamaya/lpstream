#!/bin/zsh

echo "=*=*=*=*= Start Linear Road data generation =*=*=*=*="

echo "*** Downlowd files from original GeneaLog repository ***"
echo "(wget -O h1.csv https://chalmersuniversity.box.com/shared/static/ioal17insfry4naurtybkp44dxev59ta.txt)"
wget -O h1.csv https://chalmersuniversity.box.com/shared/static/ioal17insfry4naurtybkp44dxev59ta.txt

echo "*** Increase data volume ***"
echo "(python increaseDataVol.py $1)"
python increaseDataVol.py $1

echo "=*=*=*=*= End Linear Road data generation =*=*=*=*="