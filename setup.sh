#!/usr/bin/env bash
mvn clean package
mkdir plugins
mv target/deer-slipo-plugin-1.0.0-plugin.jar plugins/
curl -L https://github.com/dice-group/deer/releases/download/2.0.1/deer-cli-2.0.1.jar -O
## use following command to execute configuration, requires fused.nt to be in the directory.
#java -jar deer-cli-2.0.1.jar configurations/slipo-bucharest.ttl