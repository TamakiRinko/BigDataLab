package example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class JoinMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
    private String fileName;

    @Override
    protected void setup(Context context) {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        fileName = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        OrderBean orderBean = null;
        String[] values = value.toString().split(" ");
        if(fileName.equals("product.txt")){
            orderBean = new OrderBean("", "", values[0], values[1], values[2], "");
        }else{
            orderBean = new OrderBean(values[0], values[1], values[2], "", "", values[3]);
        }
        context.write(orderBean, NullWritable.get());
    }
}
