import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ListMoviesByGenre 
{

    public static class Map extends Mapper<LongWritable, Text, Text, NullWritable> 
    {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {
	    Configuration conf = context.getConfiguration();
            String genre = conf.get("movieGenreParam").toLowerCase(); //Command Line Argument
            String line=value.toString();
            String [] tokenizedLine=line.split("::");
            String movieTitle=tokenizedLine[1];
            String genreValue=tokenizedLine[2].toLowerCase();
            if(genreValue.contains(genre))
            {
                context.write(new Text(movieTitle),NullWritable.get());
            }  
       
	}
    } 
	
    public static class Reduce extends Reducer<Text, NullWritable, Text,NullWritable> 
    {
        public void reduce(Text key, NullWritable value, Context context) throws IOException, InterruptedException 
        {
            context.write(key,NullWritable.get());
        }
    }
	        
    public static void main(String[] args) throws Exception 
    {
	Configuration conf = new Configuration();        
	conf.set("movieGenreParam", args[2]); //Set arguments to be given to mapper taking value from commandline
        
        Job job = new Job(conf, "ListMoviesByGenre");
	job.setOutputKeyClass(Text.class);
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputValueClass(NullWritable.class);
        
        job.setJarByClass(ListMoviesByGenre.class);
	job.setMapperClass(Map.class);
	job.setCombinerClass(Reduce.class);
	job.setReducerClass(Reduce.class);
	        
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.waitForCompletion(true);
    }
}
