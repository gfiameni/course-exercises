##############################################
# CONF
bin="$HADOOP_HOME/bin/hadoop"

datadir="data"
out="out$datadir"

d="data/ngs"
m="ngs/hs/hsmapper.py"
r="ngs/hs/hsreducer.py"
file="$d/test.sam"


##############################################
# PYTHON TESTS

# # Debug python code: only mapper
#head -1000 $file | python $m
# # Debug python code: mapper & reducer
#head -2000 $file | python $m | sort | python $r
# # Debug python code: write to output
#cat $file | python hsmapper.py | sort | python hsreducer.py > $d/test.out

##############################################
# REAL HADOOP STREAMING

# Suppress warnings
export HADOOP_HOME_WARN_SUPPRESS="TRUE"

# Clean previous output
tmp=`$bin fs -rmr $out 2>&1`

# Create dir and copy file
tmp=`$bin fs -mkdir $datadir 2>&1`
tmp=`$bin fs -put $file $datadir/ 2>&1`

echo "Data init completed"

$bin jar $HADOOP_HOME/contrib/streaming/hadoop-streaming-1.2.1.jar \
    -input $datadir \
    -mapper $m \
    -file $m \
    -reducer $r \
    -file $r \
    -output $out

$bin fs -ls $out

echo "Done"
exit