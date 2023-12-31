package csc369;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class CountryCount {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class ReducerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable result = new IntWritable();
    
        @Override
	protected void reduce(Text hostname, Iterable<IntWritable> intOne,
			      Context context) throws IOException, InterruptedException {
            int sum = 0;
            Iterator<IntWritable> itr = intOne.iterator();
        
            while (itr.hasNext()){
                sum  += itr.next().get();
            }
            result.set(sum);
            context.write(hostname, result);
       }
    }

    public static class CountryMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value,
                           Context context) throws IOException, InterruptedException {
            String[] sa = value.toString().split(" ");
            Text hostname = new Text();
            hostname.set(sa[0]);
            Text country = new Text();
            country.set(sa[1]);
            context.write(hostname, country);
        }
    }

    public static class LogMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final IntWritable one = new IntWritable(1);
        @Override
        public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
            String text[] = value.toString().split(" ");
            Text hostname = new Text();
            hostname.set(text[0]);
            context.write(hostname, one);
        }
    }

    //  Reducer: just one reducer class to perform the "join"
    public static class JoinReducer extends  Reducer<Text, Text, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)  throws IOException, InterruptedException {
            Text country = new Text();
            //IntWritable count;
            int sum = 0;
            for (Text val : values) {
                if (Character.isDigit(val.toString().charAt(0))){
                    sum += Integer.parseInt(val.toString());
                } else {
                    country = val;
                }
            }
            context.write(country, new IntWritable(sum));
        }
    }

}
