#! /bin/sh

export JAVA_OPTS="-Xmx2000m"

version="1.0"
scalaVersion="2.11"

filePath="$PWD"
fileName="Spark"
Class="SparkCore"

jarPath=$filePath/target/scala-$scalaVersion/$fileName-assembly-$version.jar
master=master
mesos_master=master
mesos_dispatcher=master

if [  -z "$1"  ]
then
    echo "Parameter error, please set local/master/mesos/mesos/cluster"
    echo "will start at local"
    spark-submit --class $Class --master local[4] $jarPath
fi

if [ "$1" = "local" ]
then
    echo "start at local"
    spark-submit --class $Class --master local[4] $jarPath
fi


if [ "$1" = "master" ]
then
    echo "start at spark://master:7077"
    spark-submit --class $Class --master spark://service:7077 $jarPath
fi

if [ "$1" = "mesos" ]
then
    echo "start at mesos://master:5050"
    spark-submit --class $Class --master mesos://$mesos_master:5050 $jarPath
fi

if [ "$1" = "mesos_cluster" ]
then
    echo "start at mesos://dispatcher:7077"
    spark-submit --class $Class --master mesos://$mesos_dispatcher:7077 --deploy-mode cluster $jarPath
fi
