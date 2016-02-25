#! /bin/sh

version="0.2"
scalaVersion="2.10"

filePath="$PWD"
jarPath=$filePath/target/scala-$scalaVersion/SparkMain-assembly-$version.jar
Class="SimpleApp"
master=master
mesos_master=master
mesos_dispatcher=master

if [  -z "$1"  ]
then
    echo "Parameter error, please set local/master/mesos/mesos/cluster"
fi

if [ "$1" = "local" ]
then
    echo "start at local"
    spark-submit --class $Class --master local[4] $jarPath
fi


if [ "$1" = "master" ]
then
    echo "start at spark://192.168.1.70:7077"
    spark-submit --class $Class --master spark://$master:7077 $jarPath
fi

if [ "$1" = "mesos" ]
then
    echo "start at mesos://192.168.1.70:5050"
    spark-submit --class $Class --master mesos://$mesos_master:5050 $jarPath
fi

if [ "$1" = "mesos_cluster" ]
then
    echo "start at mesos://dispatcher:7077"
    spark-submit --class $Class --master mesos://$mesos_dispatcher:7077 --deploy-mode cluster $jarPath
fi
