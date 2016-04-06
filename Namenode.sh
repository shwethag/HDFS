#!/bin/bash

cd bin

protoc -I=./ --java_out=./ ./hdfs.proto
javac -d . ../src/*.java -classpath ../lib/protobuf-java-2.5.0.jar:.


rmic NameNode
java -Djava.security.policy=java.policy -cp ../lib/protobuf-java-2.5.0.jar:. NameNode