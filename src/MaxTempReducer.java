/**
 * 
 */
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * @author lalit
 *
 */
public class MaxTempReducer extends Reducer<Text, LongWritable, Text, Intwritable> 
{
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Contextcontext) throws IOException, InterruptedException 
	{
		int maxTemp = Integer.MIN_VALUE;
		for (IntWritable value: values) {
			maxTemp = Math.max(maxTemp, value.get());
		}
		context.write(key, new IntWritable(maxTemp));
 	}
	
	
}
