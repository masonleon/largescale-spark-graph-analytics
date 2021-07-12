# CS6240 Group 10 Project

Fall 2020

Visit https://masonleon.github.io/largescale-spark-graph-analytics/ for additional project information.

Code author
-----------
April Gustafson, Mason Leon, Matthew Sobkowski

Installation
------------
These components are installed:
- OpenJDK 1.8.0_265
- Scala 2.11.12
- Hadoop 2.9.1
- Spark 2.3.1 (without bundled Hadoop)
- Maven 3.6.3
- AWS CLI (for EMR execution)

Dataset
-------

https://snap.stanford.edu/data/soc-LiveJournal1.html

To download to input dir:
	
	```
	bash ./data-download.sh
	```

Environment
-----------
1) Example ~/.bash_aliases:  
	```
	export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
	export HADOOP_HOME=$HADOOP_HOME/hadoop/hadoop-2.9.1  
	export SCALA_HOME=$SCALA_HOME/scala/scala-2.11.12  
	export SPARK_HOME=$SPARK_HOME/spark/spark-2.3.1-bin-without-hadoop  
	export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop  
	export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SCALA_HOME/bin:$SPARK_HOME/bin  
	export SPARK_DIST_CLASSPATH=$(hadoop classpath)    
	```   

2) Explicitly set JAVA_HOME in $HADOOP_HOME/etc/hadoop/hadoop-env.sh:  
   ```
	export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 
   ```

3) [Optional] Setup Docker Environment  
   https://docs.docker.com/get-docker/


Execution  
---------  
All of the build & execution commands are organized in the Makefile.  
1) Unzip project file.  
2) Open command prompt.  
3) Navigate to directory where project files unzipped.  
4) Edit the Makefile to customize the environment at the top.  
	Sufficient for standalone: hadoop.root, jar.name, local.input  
	Other defaults acceptable for running standalone.  
5) Standalone Hadoop:  
	```make switch-standalone```	-- set standalone Hadoop environment (execute once)  
	```make local```  
6) Pseudo-Distributed Hadoop: (https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation)  
	```make switch-pseudo```		-- set pseudo-clustered Hadoop environment (execute once)  
	```make pseudo```				-- first execution  
	```make pseudoq```				-- later executions since namenode and datanode already running  
7) AWS EMR Hadoop: (you must configure the emr.* config parameters at top of Makefile)  
	```make upload-input-aws```		-- only before first execution  
	```make aws```					-- check for successful execution with web interface (aws.amazon.com)  
	```download-output-aws```		-- after successful execution & termination
8) Docker Jupyter Scala/Spark Almond Notebook: (https://github.com/almond-sh/almond)
	```make run-container-spark-jupyter-almond``` -- run docker container with scala + spark kernel for local standalone copy token from terminal and paste in browser http://127.0.0.1:8888/?token=<TOKEN_FROM_TERMINAL>
9) Docker Standalone Hadoop/Spark
   	```make run-container-spark-jar-local``` -- run docker container environment with compiled .jar app
    ```make run-container-spark-jar-local 2>&1 | tee logs/logfile.log``` -- run docker container environment with compiled .jar app and redirect standard error+output to log

