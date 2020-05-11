package example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SecondSortMapper extends Mapper<LongWritable, Text, IntPair, NullWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split("\t");
        IntPair intPair = new IntPair(Integer.parseInt(values[0]), Integer.parseInt(values[1]));
        context.write(intPair, NullWritable.get());
    }
}
