#!/bin/bash

cd bin
rmic DataNode
rmiregistry &
javac -d . ../src/*.java -classpath ../lib/protobuf-java-2.5.0.jar:.
java -Djava.security.policy=java.policy -cp ../lib/protobuf-java-2.5.0.jar:. DataNode $1