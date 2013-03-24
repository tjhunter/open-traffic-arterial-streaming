open-traffic-arterial-streaming
===============================

An estimation algorithm using Spark Streaming.

This code runs an Expectation-Maximization procedure over some taxi data to compute some travel times distributions across a road network.

Here is a complete procedure to run the code from scratch.

Prerequisites (local)
-----------------------

The code has been tested with scala 2.9.x and spark 0.7.0. I assume you have the following libraries and programs installed:

- simple-build-tool (sbt) version >= 0.12
- spark installed from sources version == 0.7.0 (may work with some later versions)

All the other dependencies should be pulled by sbt.

Compiling (local)
------------------

Compiling should be a matter of:

```bash
sbt compile
```

Then all the tests should pass:

```bash
sbt test
```

Running with Spark on EC2
--------------------------

This is the recommended procedure as it does not require any particular setup on the local machine like HDFS, etc.

Download spark and deploy a cluster. Version 0.7 is the one used here.

```bash
wget http://spark-project.org/files/spark-0.7.0-sources.tgz
tar xvfz spark-0.7.0-sources.tgz
cd spark-0.7.0/ec2/
./spark-ec2 -s 1 -t m2.2xlarge -i ~/.ssh/KEY_NAME.pem -k KEY_NAME launch streaming-test
```

Log into the remote master and compile and deploy the project.

```bash
./spark-ec2 -s 1 -t m2.2xlarge -i ~/.ssh/KEY_NAME.pem -k KEY_NAME login streaming-test
```

In the remote machine, compile and deploy the project:

```bash
yum update
yum install http://scalasbt.artifactoryonline.com/scalasbt/sbt-native-packages/org/scala-sbt/sbt//0.12.2/sbt.rpm
git clone git@github.com:tjhunter/open-traffic-arterial-streaming.git
cd open-traffic-arterial-streaming
sbt update compile test
~/spark-ec2/copy-dir ~/.ivy2
~/spark-ec2/copy-dir /root/open-traffic-arterial-streaming/
```

Get the data and store it into the local HDFS. If you want to run a simple experiment, you do not need all the data.
The easiest is to remove most of data that will not be used.

```bash
mkdir -p /mnt/data/
cd /mnt/data/
wget www.eecs.berkeley.edu/~tjhunter/taxi_data/data_cabspotting.tar.lzma
lzma -cd data_cabspotting.tar.lzma | tar xvf -
mv data_cabspotting/* /mnt/data/

cd /mnt/data/cabspotting/
cd viterbi_slices_nid108_arterial
rm -f 2009*
rm -f 2011*
rm -f 2010-0[1^3-9]*
rm -f 2010-1*
rm -f 2010-02-*-1[0-9][0-9].*
rm -f 2010-02-*-[0-9].*
rm -f 2010-02-*-[0-5^8-9][0-9].*

~/persistent-hdfs/bin/hadoop fs -put /mnt/data/ /data/
```

At this point, all the data you need should be loaded in HDFS. The deployment procedure has not really been optimized and it is 
a bit brittle. If someone has a suggestion on how to make in better, please send me a message or a pull request.

The code dependencies and the location of the data are passed to the main program using some environment variables. 
The one variable you have to pass manually is the address of the Spak cluster. It is of the form `spark://IP:PORT`.

```bash
export SPARK_MASTER=...
```

```bash
cd /root/open-traffic-arterial-streaming
export SBT_CLASSPATH=`sbt get-jars | grep ivy`
export SPARK_CLASSPATH=$SBT_CLASSPATH:$SCALA_HOME/lib/scala-library.jar
export SPARK_HOME=/root/spark/
export EXTERNAL_HOSTNAME=`cat /root/mesos-ec2/masters`
export HDFS_DATA_DIR="hdfs://$EXTERNAL_HOSTNAME:9010/data/"
export NUM_MESOS_NODES=`cat /root/mesos-ec2/slaves | wc -l`
export SPARK_JAVA_OPTS="-Dmesos.hostname=$SPARK_MASTER -Dsocc.hdfs.root=$HDFS_DATA_DIR -Dmesos.nodes.count=$NUM_MESOS_NODES"
```

Now, the main spark program can be run:

```bash
~/spark/run arterial_research.socc.experiments.Streaming5A 
```