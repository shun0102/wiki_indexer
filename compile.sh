export HADOOP_HOME=/usr/lib/hadoop-0.20/
export DIR=build_classes

for f in $HADOOP_HOME/*.jar; do
    CLASSPATH=${CLASSPATH}:$f;
done
for f in $HADOOP_HOME/lib/*.jar; do
    CLASSPATH=${CLASSPATH}:$f;
done
for f in $HADOOP_HOME/contrib/streaming/*.jar; do
    CLASSPATH=${CLASSPATH}:$f;
done

rm -fR $DIR
mkdir $DIR

javac -classpath $CLASSPATH -d $DIR src/net/shun/mapreduce/lib/input/*.java
jar -cvf custom.jar -C $DIR .
CLASSPATH=${CLASSPATH}:$HOME/work/hadoop/custom.jar;

javac -classpath $CLASSPATH -d $DIR src/net/shun/WikiIndexer.java
jar -cvf custom.jar -C $DIR .
