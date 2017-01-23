cd ..
mvn clean
mvn package -Pcluster
scp target/java-udf-1.0-SNAPSHOT-jar-with-dependencies.jar sundongheng@10.103.8.3:/home/sundongheng/udf
scp script/keywords2Fromidscore.pig sundongheng@10.103.8.3:/home/sundongheng/udf
