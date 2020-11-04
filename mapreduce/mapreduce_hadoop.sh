# Make user directory
${HADOOP_HOME}/bin/hdfs dfs -mkdir /user
${HADOOP_HOME}/bin/hdfs dfs -mkdir /user/jack


# Remove existing input/output dirs
${HADOOP_HOME}/bin/hdfs dfs -rm -r input
${HADOOP_HOME}/bin/hdfs dfs -rm -r output

# Copy input files into dfs
${HADOOP_HOME}/bin/hdfs dfs -mkdir input
${HADOOP_HOME}/bin/hdfs dfs -put -f mapreduce/input/* input

# Run mapreduce
# grep
#${HADOOP_HOME}/bin/hadoop jar ${HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.0.jar grep input output 'dfs[a-z.]+'
# wordcount
${HADOOP_HOME}/bin/hadoop jar ${HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.0.jar wordcount input output

# View file on dfs
#${HADOOP_HOME}/bin/hdfs dfs -cat output/*

# Bring files back to local folder
${HADOOP_HOME}/bin/hdfs dfs -get -f output .
cat output/*