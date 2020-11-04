# Make user directory
${HADOOP_HOME}/bin/hdfs dfs -mkdir /user
${HADOOP_HOME}/bin/hdfs dfs -mkdir /user/jack


# Remove existing input/output dirs
${HADOOP_HOME}/bin/hdfs dfs -rm -r input

# Copy input files into dfs
${HADOOP_HOME}/bin/hdfs dfs -mkdir input
${HADOOP_HOME}/bin/hdfs dfs -mkdir output
${HADOOP_HOME}/bin/hdfs dfs -put -f mapreduce/input/ml-latest-small/* input
${HADOOP_HOME}/bin/hdfs dfs -put -f ~/Desktop/ca4022/mapreduce/genre_col_to_row.py input
${HADOOP_HOME}/bin/hdfs dfs -rm -f input/movies.csv
${HADOOP_HOME}/bin/hdfs dfs -put -f output/* input

# Run hive command
hive -f mapreduce/movielens_analysis.hql


cd /home/jack/Desktop/ca4022/output/hive_analysis

for D in * ;
do
    mv $D/000000_0 $D/$D.csv
done

# hdfs -cat hdfs:///user/hive/warehouse/mostRated/* > ~/output.csv
# for D in `find hdfs:///user/hive/warehouse -type d`
# do
#     mv $D/* > /home/jack/Desktop/ca4022/output/hive_analysis/$D.csv
# done
