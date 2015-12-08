import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Created by marwen on 08/12/15.
 */
public class TweetMap extends Mapper<Objects, BSONObject, Text, Text> {
    protected void map(Objects key, BSONObject value, Mapper.Context context) throws IOException, InterruptedException {
        String val = (String) value.get("word");
        char[] tweet = val.toLowerCase().toCharArray();
        Arrays.sort(tweet);
        context.write(new Text(new String(tweet)), new Text(val));

    }
}
