# Makefile for Group10 Spark project.

# Customize these paths for your environment.
# -----------------------------------------------------------
spark.root=/Users/apriljoy/spark/spark-2.3.1-bin-without-hadoop
hadoop.root=/Users/apriljoy/hadoop/hadoop-2.9.2
project.name=group10-project
app.name=TBD
jar.name=${project.name}.jar
maven.jar.name=${project.name}-1.0.jar
job.name=TBD
local.master=local[4]
local.output=output
local.log=log

# Pseudo-Cluster Execution
hdfs.user.name=group10
hdfs.input=input
hdfs.output=output

# AWS EMR Execution
aws.emr.release=emr-5.17.0
aws.bucket.name=group10-${project.name}
aws.input=input
aws.output=output
aws.log.dir=log
aws.num.nodes=5
aws.instance.type=m5.xlarge

# Docker Execution
docker.notebooks=notebooks
docker.container.name=
docker.container.base=
docker.container.base.img=

# -----------------------------------------------------------

# Compiles code and builds jar (with dependencies).
jar:
	mvn clean package
	cp target/${maven.jar.name} ${jar.name}

# Removes local output directory.
clean-local-output:
	rm -rf ${local.output}*

# Removes local output directory.
clean-local-log:
	rm -rf ${local.log}*

# Runs standalone
local: jar clean-local-output
	spark-submit \
		--class ${job.name} \
		--master ${local.master} \
		--name "${app.name}" \
		${jar.name} \
			${local.k} \
			${local.iterations} \
			${local.output}

# Start HDFS
start-hdfs:
	${hadoop.root}/sbin/start-dfs.sh

# Stop HDFS
stop-hdfs:
	${hadoop.root}/sbin/stop-dfs.sh

# Start YARN
start-yarn: stop-yarn
	${hadoop.root}/sbin/start-yarn.sh

# Stop YARN
stop-yarn:
	${hadoop.root}/sbin/stop-yarn.sh

# Reformats & initializes HDFS.
format-hdfs: stop-hdfs
	rm -rf /tmp/hadoop*
	${hadoop.root}/bin/hdfs namenode -format

# Initializes user & input directories of HDFS.
init-hdfs: start-hdfs
	${hadoop.root}/bin/hdfs dfs -rm -r -f /user
	${hadoop.root}/bin/hdfs dfs -mkdir /user
	${hadoop.root}/bin/hdfs dfs -mkdir /user/${hdfs.user.name}
	${hadoop.root}/bin/hdfs dfs -mkdir /user/${hdfs.user.name}/${hdfs.input}

# Removes hdfs output directory.
clean-hdfs-output:
	${hadoop.root}/bin/hdfs dfs -rm -r -f ${hdfs.output}*

# Download output from HDFS to local.
download-output-hdfs:
	mkdir ${local.output}
	${hadoop.root}/bin/hdfs dfs -get ${hdfs.output}/* ${local.output}

# Runs pseudo-clustered (ALL). ONLY RUN THIS ONCE, THEN USE: make pseudoq
# Make sure Hadoop  is set up (in /etc/hadoop files) for pseudo-clustered operation (not standalone).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation
pseudo: jar stop-yarn format-hdfs init-hdfs start-yarn clean-local-output
	spark-submit \
		--class ${job.name} \
		--master yarn \
		--deploy-mode cluster \
		${jar.name} \
			${local.k} \
			${local.iterations} \
			${local.output}
	make download-output-hdfs

# Runs pseudo-clustered (quickie).
pseudoq: jar clean-local-output clean-hdfs-output
	spark-submit \
		--class ${job.name} \
		--master yarn \
		--deploy-mode cluster \
		${jar.name} \
			${local.k} \
			${local.iterations}
	make download-output-hdfs

# Create S3 bucket.
make-bucket:
	aws s3 \
		mb s3://${aws.bucket.name}

# Upload data to S3 input dir.
upload-input-aws: make-bucket
	aws s3 \
		sync ${local.input} s3://${aws.bucket.name}/${aws.input}

# Delete S3 output dir.
delete-output-aws:
	aws s3 \
		rm s3://${aws.bucket.name}/ \
			--recursive \
			--exclude "*" \
			--include "${aws.output}*"

# Upload application to S3 bucket.
upload-app-aws:
	aws s3 \
		cp ${jar.name} s3://${aws.bucket.name}

# Main EMR launch.
aws: jar upload-app-aws delete-output-aws
	aws emr \
		create-cluster \
			--name "${project.name} Cluster" \
			--release-label ${aws.emr.release} \
			--instance-groups '[' \
				'{' \
					'"InstanceCount":${aws.num.nodes},' \
					'"InstanceGroupType":"CORE",' \
					'"InstanceType":"${aws.instance.type}"' \
				'},' \
				'{' \
					'"InstanceCount":1,' \
					'"InstanceGroupType":"MASTER",' \
					'"InstanceType":"${aws.instance.type}"' \
				'}' \
			']' \
			--applications \
				Name=Hadoop \
				Name=Spark \
			--steps \
				Type=CUSTOM_JAR, \
				Name="${app.name}", \
				Jar="command-runner.jar", \
				ActionOnFailure=TERMINATE_CLUSTER, \
				Args=[ \
					"spark-submit", \
						"--deploy-mode", "cluster", \
						"--class","${job.name}", \
						"s3://${aws.bucket.name}/${jar.name}", \
							"s3://${aws.bucket.name}/${aws.input}", \
							"s3://${aws.bucket.name}/${aws.output}" \
				] \
			--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
			--use-default-roles \
			--enable-debugging \
			--auto-terminate

# Download output from S3.
download-output-aws: clean-local-output clean-local-log
	mkdir ${local.output}
	aws s3 \
		sync s3://${aws.bucket.name}/${aws.output} ${local.output}
	mkdir ${local.log}
	aws s3 \
		sync s3://${aws.bucket.name}/${aws.log.dir} ${local.log}

# Change to standalone mode.
switch-standalone:
	cp config/standalone/*.xml ${hadoop.root}/etc/hadoop

# Change to pseudo-cluster mode.
switch-pseudo:
	cp config/pseudo/*.xml ${hadoop.root}/etc/hadoop

# Package for release.
distro:
	rm -f ${project.name}.tar.gz
	rm -f ${project.name}.zip
	rm -rf build
	mkdir -p build/deliv/${project.name}
	cp -r src build/deliv/${project.name}
	cp -r config build/deliv/${project.name}
	cp -r input build/deliv/${project.name}
	cp pom.xml build/deliv/${project.name}
	cp Makefile build/deliv/${project.name}
	cp README.txt build/deliv/${project.name}
	tar -czf ${project.name}.tar.gz -C build/deliv ${project.name}
	cd build/deliv && zip -rq ../../${project.name}.zip ${project.name}

docker-build-local-container:
	if [[ "$(docker images -q ${container.base}/${container.base.img}:latest 2> /dev/null)" != "" ]]; then \
		docker rmi ${container.base}/${container.base.img}:latest; \
	fi
	docker build \
		--tag ${container.base}/${container.name} \
		--target ${container.name} \
		.

docker-run-local-container: jar clean-local-output docker-build-local-container
	docker run \
		-it \
		--rm \
		-p 50070:50070 \
		-p 8088:8088 \
		-p 9870:9870 \
		-p 9864:9864 \
		-v ${PWD}/input:/input/ \
		-v ${PWD}/output:/result/ \
		--name=${container.name} \
		${container.base}/${container.name}:latest