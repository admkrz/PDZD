import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;

public class JoinMapperInspections extends Mapper<LongWritable, Text, Text, HashMap<String,String>> {
    public void map(LongWritable k, Text value, Context context)
            throws IOException, InterruptedException {

        String[] words = value.toString().split(",");
        HashMap<String, String> values = new HashMap<String, String>();
        values.put("Bussiness Name", words[2]);
        values.put("Inspection Date", words[3]);
        values.put("Inspection Result", words[4]);
        values.put("Borough", words[6]);
        values.put("Building Number", words[7]);
        values.put("Street", words[8]);
        values.put("Street 2", words[9]);
        values.put("Unit Type", words[10]);
        values.put("Unit", words[11]);
        values.put("Description", words[12]);
        values.put("City", words[13]);
        values.put("State", words[14]);
        values.put("Zip", words[15]);
        context.write(new Text(words[0]), values);
    }
}