#!/bin/bash 

spark-submit --class ShortestPaths --master local[4] --name "ShortestPaths" group10-project.jar input output
cp -a /output/* /result/

