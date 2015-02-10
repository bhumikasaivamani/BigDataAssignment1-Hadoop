
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author bhumikasaivamani
 */
public class MaleUsersBelow7Years 
{
    public static class Map extends Mapper<LongWritable,Text,Text,Text>
    {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {
            String line=value.toString();
            String [] tokenizedLine=line.split("::");
            String userId=tokenizedLine[0];
            String gender=tokenizedLine[1];
            String age=tokenizedLine[2];
            if(gender.compareTo("M")==0)
            {
                context.write(new Text(userId),new Text(age));
            }
        }
        
        
    }
    
    public static class Reduce extends Reducer<Text, Text, Text, Text> 
    {
        public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException
        {
           
           for(Text v : values)
           {
               String ageValue=v.toString();
               if(Integer.parseInt(ageValue)<=7)
               {
                  context.write(key,new Text(ageValue)); 
               }
           }
           
        }
        
    }
    
    
    public static void main(String args[])throws Exception
    {
        Configuration configuration=new Configuration();
        
        Job job = new Job(configuration, "CountMaleUsersLessThan7");
        job.setJarByClass(MaleUsersBelow7Years.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reducer.class);
        job.setCombinerClass(Reducer.class);
        
        
        job.setInputFormatClass(TextInputFormat.class);
 	job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}
