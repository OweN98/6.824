#!/bin/bash
for((i=0; i<30; i++))
do
  go test -run TestRejoin2B
done