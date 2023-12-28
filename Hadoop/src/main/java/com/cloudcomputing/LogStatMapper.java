package com.cloudcomputing;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class LogStatMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Pattern ipPattern = Pattern.compile("(\\d+\\.\\d+\\.\\d+\\.\\d+).*?\\[(\\d{2})/\\w{3}/\\d{4}:(\\d{2}):.*?\\+\\d{4}\\]");
    private int from;
    private int to;
    private boolean flagTime;
    protected void setup(Context context){
        Configuration config = context.getConfiguration();
        String timeRange = config.get("timeRange");
        if (timeRange != null) {
            flagTime = true;
            String[] hours = timeRange.split("-");
            this.from = Integer.parseInt(hours[0]);
            this.to = Integer.parseInt(hours[1]);
        } else {
            flagTime = false;
        }
    }
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        Matcher matcher = ipPattern.matcher(value.toString());
        if (matcher.find()) {
            int hour = Integer.parseInt(matcher.group(3));
            if (!flagTime || (hour >= this.from && hour < this.to)) {
                String ip = matcher.group(1);
                context.write(new Text("IP - "+ip + "\t Hour - " + hour), new IntWritable(1));
            }
        }
    }
}