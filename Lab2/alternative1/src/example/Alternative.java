package example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

public class Alternative {
    // 重写Map
    public static class AlternativeMapper extends Mapper<Object, Text, DoubleWritable, Text>{
        // 实现map()函数
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // key: lineno, value: line string
            double frequency = Double.parseDouble(value.toString().split("\t")[1].split(",")[0]);
            context.write(new DoubleWritable(frequency), value);
        }
    }

    // 重写Reducer
    public static class AlternativeReducer extends Reducer<DoubleWritable, Text, Text, Text>{
        //实现reduce()函数
        public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value: values){
                context.write(new Text(value.toString().split("\t")[0]), new Text(value.toString().split("\t")[1]));
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //设定配置文件
        Configuration configuration = new Configuration();
        //命令行参数
        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
        if(otherArgs.length != 2){
            System.out.println("Usage: alternative1 <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(configuration, "alternative");
        job.setJarByClass(Alternative.class);
        job.setMapperClass(AlternativeMapper.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(AlternativeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
