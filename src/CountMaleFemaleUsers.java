import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CountMaleFemaleUsers 
{
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> 
    {
        private final static IntWritable one = new IntWritable(1);
        //Mapper takes value from data set line by line and produces key value pair for each data row in the data set
    	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {
	    String line=value.toString();
            String [] tokenizedLine=line.split("::");
            String gender=tokenizedLine[1];
            String ageValue=tokenizedLine[2];
            //Take Class of Age and Gender and make it as a key
            String keyAgeClass=ageValue.concat(" ").toString().concat(gender);
            context.write(new Text(keyAgeClass),one);
	 } 
    }
    
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> 
    {
        private IntWritable total = new IntWritable();
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
        {
	    int sum=0;
            for (IntWritable val : values) 
            {
                sum=sum+val.get();
            }
            total.set(sum);
            context.write(key, total); //write to ouput file
        }
    }
	        
    public static void main(String[] args) throws Exception 
    {
	Configuration conf = new Configuration();        
	Job job = new Job(conf, "CountMaleFemaleUsers");
	    
	job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
        
	job.setJarByClass(CountMaleFemaleUsers.class);
	job.setMapperClass(Map.class);
	job.setCombinerClass(Reduce.class);
	job.setReducerClass(Reduce.class);
	        
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        
	job.waitForCompletion(true);
    }
}
