package example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class JoinPartitioner extends Partitioner<OrderBean, NullWritable> {
    // 自定义比较pId % 4的partitioner
    private Partitioner<IntWritable, NullWritable> partitioner= new Partitioner<IntWritable, NullWritable>() {
        @Override
        public int getPartition(IntWritable intWritable, NullWritable nullWritable, int i) {
            // 4个Reducer
            return intWritable.get() % 4;
        }
    };
    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int i) {
        return partitioner.getPartition(new IntWritable(Integer.parseInt(orderBean.getpId())), nullWritable, i);
    }
}
