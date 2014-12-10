datadir="data"
out="out$datadir"
file="sam/a.sam"
bin="$HADOOP_HOME/bin/hadoop"

##############################################
# PYTHON TESTS

# # Debug python code: only mapper
# head -1000 $file | python hsmapper.py
# # Debug python code: mapper & reducer
# head -2000 $file | python hsmapper.py | sort | python hsreducer.py
# # Debug python code: write to output
# cat $file | python hsmapper.py | sort | python hsreducer.py > test2.out

##############################################
# REAL HADOOP STREAMING

# Clean previous output
$bin fs -rmr $out

# Create dir and copy file
$bin fs -mkdir $datadir
$bin fs -put $file $datadir/

$bin jar $HADOOP_HOME/contrib/streaming/hadoop-streaming-1.2.1.jar \
    -input $datadir \
    -output $out \
    -mapper hsmapper.py \
    -reducer /bin/wc \
    -file hsmapper.py