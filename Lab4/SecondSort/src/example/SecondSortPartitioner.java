package example;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class SecondSortPartitioner extends Partitioner<IntPair, NullWritable> {

    @Override
    public int getPartition(IntPair intPair, NullWritable nullWritable, int i) {
        return intPair.getFirst();
    }
}
