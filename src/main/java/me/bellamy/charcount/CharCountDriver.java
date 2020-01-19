package me.bellamy.charcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CharCountDriver {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        // Hadoop会自动根据驱动程序的类路径来扫描该作业的Jar包。
        job.setJarByClass(CharCountDriver.class);

        // 指定mapper
        job.setMapperClass(CharCountMapper.class);
        // 指定reducer
        job.setReducerClass(CharCountReducer.class);

        // map程序的输出键-值对类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 输出键-值对类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 输入文件的路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        // 输入文件路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}
