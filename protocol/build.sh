set -eu

cd java

# create c files
for file in src/main/java/pingcap/com/*.java; do
	javac $file
	tmp=${file%\.java*}
	tmp=`basename $tmp`
	old=`pwd`
	cd src/main/java
	javah -d ../../../../c pingcap.com.$tmp
	cd $old
done

# create jar file
mvn package
mvn install:install-file -Dfile=target/MagicProto-1.0.jar -DgroupId=pingcap.com -DartifactId=MagicProto -Dversion=1.0 -Dpackaging=jar
