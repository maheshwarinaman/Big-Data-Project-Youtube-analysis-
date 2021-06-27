package youtube; 

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapred.OutputCollector;  
import org.apache.hadoop.mapred.Reporter;  
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;


public class MRdriver extends Configured implements Tool {
    @SuppressWarnings("deprecation")
   public int run(String[] args) throws Exception {
    
        // Configure  MR job
    	  Job job = new Job(getConf(), "youtube"); 
        job.setJarByClass(MRdriver.class);
        job.setMapperClass(MRmapper.class);
        job.setReducerClass(MRreducer.class);
        job.setCombinerClass(MRreducer.class);
	
    
        // Setup input and output paths for MR job
        job.setInputFormatClass(TextInputFormat.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);

        // Run  MR job syncronously with verbose output set to true
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	//job.waitForCompletion(true);
	    
        return job.waitForCompletion(true) ? 0 : 1; 
    }

   public static void main(String[] args) throws Exception { 
       if(args.length != 2) {
           System.err.println("usage: MRdriver <input-path> <output-path>");
           System.exit(1);
       }
       Configuration conf = new Configuration();
       System.exit(ToolRunner.run(conf, new MRdriver(), args));
       
   } 
}
