package example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;

public class Alternative2 {

    private static int fileNum;

    // 重写Map
    public static class AlternativeMapper extends Mapper<Text, Text, Text, Text>{
        // 实现map()函数
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            // key: wordName, value: remaining line string
            String[] terms = value.toString().split(",")[1].split(";");
            //fileNum
            int fileNum = Integer.parseInt(context.getConfiguration().get("fileNum"));
            double idf = Math.log((double)fileNum * 1.0/(terms.length + 1));

            Map<String, Integer> authorMap = new HashMap<>();

            for(String term: terms) {
                //get the author name
                String temp = term.split(":")[0];
                String author = null;
                char[] chars = temp.toCharArray();
                for (int i = 0; i < chars.length; i++) {
                    if (Character.isDigit(chars[i])) {
                        author = temp.substring(0, i);
                        break;
                    }
                }
                if(author != null){
                    // get the frequency
                    int f = Integer.parseInt(term.split(":")[1]);
                    authorMap.put(author, authorMap.getOrDefault(author,0)+f);
                }
            }

            for(Map.Entry<String, Integer> entry : authorMap.entrySet()){
                Text outkey = new Text();
                outkey.set(entry.getKey());
                if(!entry.getKey().equals("")) {
                    context.write(outkey, new Text(key + "#" + String.format("%.02f", entry.getValue() * idf)));
//                    context.write(outkey, new Text(key + "#" + String.format("%d", fileNum)));
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //设定配置文件
        Configuration configuration = new Configuration();
        //命令行参数
        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
        if(otherArgs.length != 3){
            System.out.println("Usage: alternative1 <in> <out> <in2>");
            System.exit(2);
        }
        fileNum = FileSystem.newInstance(configuration).listStatus(new Path(otherArgs[2])).length;
        configuration.set("fileNum", String.valueOf(fileNum));

        Job job = Job.getInstance(configuration, "alternative");
        job.setJarByClass(Alternative2.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapperClass(AlternativeMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
