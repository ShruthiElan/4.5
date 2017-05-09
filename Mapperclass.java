import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 


public class Mapperclass extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	Text g = new Text();
	IntWritable o = new IntWritable(1);
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] a = value.toString().split(",");
		int i = Integer.parseInt(a[1]);
		if(i==0)
                { 
			g.set(a[4]);
			context.write(g, o);
		}
	}
}