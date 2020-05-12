

cd /root/src/stream-project
git pull
mvn clean package


cp target/tj-hospital.jar ~/TJJar/
cp target/tj-hospital.jar ~/TJJar/base/
cp target/tj-hospital.jar ~/TJJar/sync/


cp target/tj-hospital.jar ~/TJJar/batch/



/root/shell/restart.sh

