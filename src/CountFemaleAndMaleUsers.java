
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
public class CountFemaleAndMaleUsers {
    
    
    public static class Map extends Mapper<Text,Text,Text,Text>
    {
        public void map(Text key, Text value, Reducer.Context context) throws IOException, InterruptedException 
        {
            String line=value.toString();
            String [] tokenizedLine=line.split("::");
            String gender=tokenizedLine[1];
            String ageValue=tokenizedLine[2];
            
            String keyAgeClass;
            int age=Integer.parseInt(ageValue);
            if(age<18)
            {
               keyAgeClass="7";
            }
            else if(age>=18 && age<=24)
            {
                keyAgeClass="24";
            }
            else if(age>=25 && age<=34)
            {
                keyAgeClass="31";
            }
            else if(age>=35 && age<=44)
            {
                keyAgeClass="41";
            }
            else if(age>=45 && age<=55)
            {
                keyAgeClass="51";
            }
            else if(age>=56 && age<=61)
            {
                keyAgeClass="56";
            }
            else 
            {
                keyAgeClass="62";
            }
           
            context.write(new Text(keyAgeClass),new Text(gender));
            
        }
        
        
    }
    
    public static class Reduce extends Reducer<Text, Text, Text, Text> 
    {
        public void reduce(Text key, Iterable<Text> values, Reducer.Context context)throws IOException, InterruptedException
        {
           String Key=key.toString();
           int countM=0;
           int countF=0;
          
           for(Text v : values)
           {
               String gender=v.toString();
               if(gender.equals("M"))
                   countM++;
               else
                   countF++;
           }
           String outputM="M "+Integer.toString(countM);
           String outputF="F "+Integer.toString(countF);
           context.write(key,new Text(outputM));
           context.write(key,new Text(outputF));
        }
        
    }
    
    
    public static void main(String args[])throws Exception
    {
        Configuration configuration=new Configuration();
        
        Job job = new Job(configuration, "CountMaleFemaleUsers");
        job.setJarByClass(CountFemaleAndMaleUsers.class);
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
