package example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class SecondSortMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //设定配置文件
        Configuration configuration = new Configuration();
        //命令行参数
        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
        if(otherArgs.length != 2){
            System.out.println("Usage: SecondSort <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(configuration, "SecondSort");
        job.setJarByClass(SecondSortMain.class);
        job.setMapperClass(SecondSortMapper.class);
        job.setMapOutputKeyClass(IntPair.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setPartitionerClass(SecondSortPartitioner.class);
        job.setReducerClass(SecondSortReducer.class);
        job.setNumReduceTasks(10);
        job.setOutputKeyClass(IntPair.class);
        job.setOutputValueClass(NullWritable.class);
        job.setGroupingComparatorClass(SecondSortComparator.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
