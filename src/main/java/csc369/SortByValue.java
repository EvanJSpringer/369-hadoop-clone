package csc369;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class SortByValue {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, IntWritable, Text> {
	private final IntWritable one = new IntWritable(1);

        @Override
	protected void map(LongWritable key, Text value,
			   Context context) throws IOException, InterruptedException {
	    String[] sa = value.toString().split("\t");
	    IntWritable valueText = new IntWritable(Integer.parseInt(sa[1]));
        Text keyText = new Text();
        keyText.set(sa[0]);
	    context.write(valueText, keyText);
        }
    }

    public static class ReducerImpl extends Reducer<IntWritable, Text, IntWritable, Text> {
	private IntWritable result = new IntWritable();
    
        @Override
	protected void reduce(IntWritable value, Iterable<Text> key,
			      Context context) throws IOException, InterruptedException {
            Iterator<Text> itr = key.iterator();
            context.write(value, itr.next());
       }
    }

}
