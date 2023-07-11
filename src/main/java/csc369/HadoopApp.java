package csc369;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HadoopApp {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Hadoop example");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

	if (otherArgs.length < 3) {
	    System.out.println("Expected parameters: <job class> <input dir> <output dir>");
	    System.exit(-1);
	} else if ("WordCount".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(WordCount.ReducerImpl.class);
	    job.setMapperClass(WordCount.MapperImpl.class);
	    job.setOutputKeyClass(WordCount.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(WordCount.OUTPUT_VALUE_CLASS);
	} else if ("AccessLog".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(AccessLog.ReducerImpl.class);
	    job.setMapperClass(AccessLog.MapperImpl.class);
	    job.setOutputKeyClass(AccessLog.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(AccessLog.OUTPUT_VALUE_CLASS);
	} else if ("AccessLog2".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(AccessLog2.ReducerImpl.class);
	    job.setMapperClass(AccessLog2.MapperImpl.class);
	    job.setOutputKeyClass(AccessLog2.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(AccessLog2.OUTPUT_VALUE_CLASS);
	} else if ("URLCount".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(URLCount.ReducerImpl.class);
		job.setMapperClass(URLCount.MapperImpl.class);
		job.setOutputKeyClass(URLCount.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(URLCount.OUTPUT_VALUE_CLASS);
	} else if ("SortByValue".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(SortByValue.ReducerImpl.class);
		job.setMapperClass(SortByValue.MapperImpl.class);
		job.setOutputKeyClass(SortByValue.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(SortByValue.OUTPUT_VALUE_CLASS);
	} else if ("CodeCount".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(CodeCount.ReducerImpl.class);
		job.setMapperClass(CodeCount.MapperImpl.class);
		job.setOutputKeyClass(CodeCount.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(CodeCount.OUTPUT_VALUE_CLASS);
	} else if ("TotalBytes".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(TotalBytes.ReducerImpl.class);
		job.setMapperClass(TotalBytes.MapperImpl.class);
		job.setOutputKeyClass(TotalBytes.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(TotalBytes.OUTPUT_VALUE_CLASS);
	} else if ("RequestCountPerClient".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(RequestCountPerClient.ReducerImpl.class);
		job.setMapperClass(RequestCountPerClient.MapperImpl.class);
		job.setOutputKeyClass(RequestCountPerClient.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(RequestCountPerClient.OUTPUT_VALUE_CLASS);
	} else if ("CalendarCount".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(CalendarCount.ReducerImpl.class);
		job.setMapperClass(CalendarCount.MapperImpl.class);
		job.setOutputKeyClass(CalendarCount.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(CalendarCount.OUTPUT_VALUE_CLASS);
	} else {
	    System.out.println("Unrecognized job: " + otherArgs[0]);
	    System.exit(-1);
	}

        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        System.exit(job.waitForCompletion(true) ? 0: 1);
    }

}
