package youtube;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import java.io.IOException;

public class MRreducer  extends Reducer <Text,Text,Text,Text> {
    public static String IFS=",";
    public static String OFS=",";
   	public void reduce(Text key, Iterable<Text> values, Context context) 
		   throws IOException, InterruptedException {
       
           // Initialize variables
	   Long mostViewed = null;
	   Long mostLiked = null;
	   Long mostDisliked = null;
	   Text viewsvideoId = null;
	   Text likesvideoId = null;
	   Text dislikesvideoId = null;
	   Text video_id = null;
	   Long views = null;
	   Long likes = null;
	   Long dislikes = null;
	   Long RRC = null;
	   Long RBRC = null;
	   Long RGRC = null;
	   
	   
       
           // Loop through values to find most viewed, most liked, and most disliked video
	   for (Text val: values)
	   {
		   String[] arrayvalue = val.toString().split(" ");
		   video_id = new Text(arrayvalue[0]);
		   views = Long.parseLong(arrayvalue[1]);
		   likes = Long.parseLong(arrayvalue[2]);
		   dislikes = Long.parseLong(arrayvalue[3]);
		   RC = Long.parseLong(arrayvalue[4]);
		   BRC = Long.parseLong(arrayvalue[5]);
		   GRC = Long.parseLong(arrayvalue[6]);
		   
		   if(views > mostViewed)
		   {
			   mostViewed = views;
			   viewsvideoId = video_id;			   
		   }
		   
		   if(likes > mostLiked)
		   {
			   mostLiked = likes;
			   likesvideoId = video_id;		
		   }
		   if(dislikes > mostDisliked)
		   {
			   mostDisliked = dislikes;
			   dislikesvideoId = video_id;
		   }
		   
	   }
	   
       
           // Write the key-value pair to the context
           context.write(new Text("Category_ID: "+ key ), new Text("most viewed"+ mostViewed+ "Video ID"+ viewsvideoId+ "most liked"+ mostLiked+ "Video ID"+ likesvideoId+ "most disliked"+ mostDisliked+ "Video ID"+ dislikesvideoId+ "Total records"+ RRC+ "Total bad records"+ RBRC+ "Total good records"+ RGRC));
   }
}
