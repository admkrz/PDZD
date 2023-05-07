import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class JoinMapperPayments extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable k, Text value, Context context)
            throws IOException, InterruptedException {

        String[] words = value.toString().split(",");
        if (words.length == 11)
            context.write(new Text(words[5]), new Text("payment:" + words[9]));
    }
}