package youtube;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;

public class MRmapper  extends Mapper <LongWritable,Text,Text,Text> {
    static String IFS=",";
    static String OFS=",";
    static int NF=11;
    
    private Text category_id = new Text();
    private Text video_id = new Text();
    private Text views = new Text();
    private Text videoslikes = new Text();
    private Text dislikes = new Text();
    private Text RCString = new Text();
    private Text BRCString = new Text();
    private Text GRCString = new Text();
    
    
    
    public void map(LongWritable key, org.w3c.dom.Text value, Context context) 
                    throws IOException, InterruptedException {
        
        /** USvideos.csv
        video_id
        title
        channel_title
        category_id
        tags
        views
        likes
        dislikes
        comment_total
        thumbnail_link
        date
        */

    	int BRC = 0;
    	int GRC = 0;
    	int RC = 0;
        StringTokenizer iterator = new StringTokenizer(value.toString(),"\\s+");
        
        // Remove schema line
        String[] temp = iterator.toString().split(" ");

        if (!temp[0].equals("video_id"))
        {
        		RC++;
        	
	        
        		// Count num fields, increment bad record counter, and return if bad
	        	if(temp.length > NF) 
	        	{
	        		BRC++;
	        	}
	        	else
	        	{
	        		GRC++;
			        // Pull out fields of interest
	        		video_id.set(temp[0]);
	        		views.set(temp[5]);
	        		videoslikes.set(temp[6]);
	        		dislikes.set(temp[7]);
	        		category_id.set(temp[3]);
	        		RCString.set(Integer.toString(RC));
	        		BRCString.set(Integer.toString(BRC));
	        		GRCString.set(Integer.toString(GRC));
	        	}
	        		
		        //Write key value pair to context
	        	context.write(category_id,new Text(video_id+ " " +views + " " +videoslikes+ " " +dislikes+ " " +RCString+ " " +BRCString+ " " +GRCString));
	        		
        } 
        
    }
}
