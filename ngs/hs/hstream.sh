##############################################
# CONF
bin="$HADOOP_HOME/bin/hadoop"
userlogs="/tmp/hadoop-root/mapred/local/userlogs"

datadir="data"
out="out$datadir"

d="data/ngs"
m="ngs/hs/hsmapper.py"
#m="ngs/hs/hsmapper_deco.py"
r="ngs/hs/hsreducer.py"

file="$d/input.sam"
file="$d/input.bz2"
#file="$d/input.bam"

##############################################
# COMPRESSION and DECOMPRESSION

# # SAM 2 BAM:
# $ samtools view -bS file.sam > file.bam
# # BAM 2 SAM:
# $ samtools view -H file.bam > file.sam
# $ samtools view file.bam >> file.sam

# # Using text file!
# filebz="$d/input.bz2"
# if [ ! -f "$file" ]; then
#     echo "No text input found"
#     echo "Decompressing"
#     bunzip2 -dvc $filebz > $file
# fi

##############################################
# NORMAL PYTHON DEBUGGING

# # Debug python code: only mapper
#head -100 $file | python $m

# # Debug python code: mapper & reducer
#head -200 $file | python $m | sort | python $r

# # Debug python code: write to output
#cat $file | python $m | sort | python $r > $d/test.out

# # If debugging, decomment exit below
#exit

##############################################
# REAL HADOOP STREAMING

# Suppress warnings
export HADOOP_HOME_WARN_SUPPRESS="TRUE"

# Clean previous output
tmp=`$bin fs -rmr $out 2>&1`

# Create dir and copy file
tmp=`$bin fs -rmr $datadir 2>&1`
tmp=`$bin fs -mkdir $datadir 2>&1`
tmp=`$bin fs -put $file $datadir/ 2>&1`

# Cleaning old logs
tmp=`rm -rf $userlogs/* 2>&1`

echo "Data init completed"

##############################################
$bin \
    jar $HADOOP_HOME/contrib/streaming/hadoop-streaming-1.2.1.jar \
    -jobconf stream.recordreader.compression=bz2 \
    -input $datadir \
    -mapper $m \
    -file $m \
    -reducer $r \
    -file $r \
    -output $out

if [ $? == "0" ]; then
    echo "Output is:"
    $bin fs -ls $out

    $bin fs -get $out/part-00000 $d/test.out
    echo "Copied output to $d/test.out"
else
    # Easier cli debug
    echo "Failed..."
    sleep 3
    cat $userlogs/job_*/*/* | less
fi

echo "Done"
