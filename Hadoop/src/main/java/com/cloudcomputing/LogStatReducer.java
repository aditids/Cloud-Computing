package com.cloudcomputing;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class LogStatReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private PriorityQueue<Map.Entry<String, Integer>> pq;
    protected void setup(Context context) {
        pq = new PriorityQueue<>(Comparator.<Map.Entry<String, Integer>>comparingInt(Map.Entry::getValue).reversed());
    }
    protected void reduce(Text key, Iterable<IntWritable> value, Context context){
        int sum = 0;
        for (IntWritable i : value) {
            sum += i.get();
        }
        pq.offer(new AbstractMap.SimpleEntry<>(key.toString(), sum));
    }
    protected void cleanup(Context context) throws IOException, InterruptedException {
        int top = 0;
        while (top < 3 && !pq.isEmpty()) {
            Map.Entry<String, Integer> entry = pq.poll();
            context.write(new Text(entry.getKey()+"\t Count - "), new IntWritable(entry.getValue()));
            top++;
        }
    }
}