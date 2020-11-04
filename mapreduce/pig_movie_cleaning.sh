# Make user directory
${HADOOP_HOME}/bin/hdfs dfs -mkdir /user
${HADOOP_HOME}/bin/hdfs dfs -mkdir /user/jack


# Remove existing input/output dirs
${HADOOP_HOME}/bin/hdfs dfs -rm -r input
${HADOOP_HOME}/bin/hdfs dfs -rm -r output

# Copy input files into dfs
${HADOOP_HOME}/bin/hdfs dfs -mkdir input
${HADOOP_HOME}/bin/hdfs dfs -mkdir output
${HADOOP_HOME}/bin/hdfs dfs -put -f mapreduce/input/ml-latest-small/* input

# Run pig command
pig -x mapreduce -f mapreduce/movielens_cleaning.pig

# Bring files back to local folder
${HADOOP_HOME}/bin/hdfs dfs -get -f output .
#rm output/.movies.csv.crc # remove gunk
#cat output/*
