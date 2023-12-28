package com.cloudcomputing;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;
public class runner {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration config = new Configuration();
        String inputPath = args[0];
        String outputPath = args[1];
        Job job = Job.getInstance(config, "logger");
        job.setJarByClass(runner.class);
        job.setMapperClass(LogStatMapper.class);
        job.setReducerClass(LogStatReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(inputPath));
        TextOutputFormat.setOutputPath(job, new Path(outputPath));
        job.waitForCompletion(true);
    }
}