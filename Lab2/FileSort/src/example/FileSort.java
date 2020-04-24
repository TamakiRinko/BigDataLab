package example;

import javafx.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

public class FileSort {

    // 重写RecordReader
    public static class FileSortRecordReader extends RecordReader<Text, Text>{
        // 当前InputSplit的文件名
        private String fileName;
        // 用于读取每行内容的默认RecordReader
        private LineRecordReader lineRecordReader = new LineRecordReader();

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            lineRecordReader.initialize(inputSplit, taskAttemptContext);
            fileName = ((FileSplit)inputSplit).getPath().getName();
            fileName = fileName.substring(0, fileName.length() - 15);
        }
        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            return lineRecordReader.nextKeyValue();
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return new Text(fileName);
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return lineRecordReader.getCurrentValue();
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return lineRecordReader.getProgress();
        }

        @Override
        public void close() throws IOException {
            lineRecordReader.close();
        }
    }

    // 重写InputFormat
    public static class FileSortInputFormat extends FileInputFormat<Text, Text>{

        @Override
        public RecordReader<Text, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            FileSortRecordReader fileSortRecordReader = new FileSortRecordReader();
            fileSortRecordReader.initialize(inputSplit, taskAttemptContext);
            return fileSortRecordReader;
        }
    }

    // 重写Map
    public static class FileSortMapper extends Mapper<Text, Text, Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        // 实现map()函数
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            // key: 文件name, value: line string
            StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
            while (stringTokenizer.hasMoreTokens()){
                word.set(stringTokenizer.nextToken());
                // Emit(<term, dn>, t)
                context.write(new Text(word.toString() + "#" + key.toString()), one);
            }
        }
    }

    // 重写Combiner，合并相同的单词
    public static class FileSortCombiner extends Reducer<Text, IntWritable, Text, IntWritable>{
        private final IntWritable result = new IntWritable();
        //实现reduce()函数
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            //迭代遍历values，得到同一key的所有value
            for (IntWritable value: values) {
                sum += value.get();
            }
            result.set(sum);
            //产生输出对<key, value>
            context.write(key, result);
        }
    }

    // 重写partitioner
    public static class FileSortPartitioner extends HashPartitioner<Text, IntWritable>{
        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            // 只拿到name
            Text term = new Text(key.toString().split("#")[0]);
            return super.getPartition(term, value, numReduceTasks);
        }
    }

    // 重写Reducer
    public static class FileSortReducer extends Reducer<Text, IntWritable, Text, Text>{

        private String tPrev;
        private List<String> postingLists;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            tPrev = null;
            postingLists = new ArrayList<>();
        }

        //实现reduce()函数
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value: values) {
                sum += value.get();
            }

            String word = key.toString().split("#")[0];
            String fileName = key.toString().split("#")[1];
            if(tPrev != null && !word.equals(tPrev)){
                context.write(new Text(tPrev), new Text(getFrequency() + "," + getString()));
                postingLists = new ArrayList<>();
            }
            String temp = fileName + ":" + sum + ";";
            postingLists.add(temp);
            tPrev = word;
        }

        public double getFrequency(){
            int wordNum = 0;
            int fileNum = 0;
            for(String posting: postingLists){
                fileNum += 1;
                String num = posting.split(":")[1];
                wordNum += Integer.parseInt(num.substring(0, num.length() - 1));
            }
            return wordNum * 1.0 / fileNum;
        }

        public String getString(){
            StringBuilder stringBuilder = new StringBuilder();
            for(String posting: postingLists){
                stringBuilder.append(posting);
            }
            return stringBuilder.toString();
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text(tPrev), new Text(getFrequency() + "," + getString()));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //设定配置文件
        Configuration configuration = new Configuration();
        //命令行参数
        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
        if(otherArgs.length != 2){
            System.out.println("Usage: FileSort <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(configuration, "file sort");
        job.setJarByClass(FileSort.class);
        job.setMapperClass(FileSortMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setCombinerClass(FileSortCombiner.class);
        job.setPartitionerClass(FileSortPartitioner.class);
        job.setReducerClass(FileSortReducer.class);
        job.setInputFormatClass(FileSortInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
