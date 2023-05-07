import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;

public class JoinMapperFines extends Mapper<LongWritable, Text, Text, HashMap<String,String>> {
    public void map(LongWritable k, Text value, Context context)
            throws IOException, InterruptedException {

        String[] words = value.toString().split(",");
        HashMap<String, String> values = new HashMap<String, String>();
        values.put("FEE SEQUENCE ID", words[5]);
        values.put("FEE TYPE", words[6]);
        values.put("FEE DESCRIPTION", words[7]);
        values.put("FEE AMOUNT", words[8]);
        values.put("FEE DATE", words[9]);
        context.write(new Text(words[0]), values);
    }
}