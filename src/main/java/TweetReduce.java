import com.mongodb.BasicDBObjectBuilder;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BSONObject;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by marwen on 08/12/15.
 */
public class TweetReduce extends Reducer<Text, Text, ObjectId, BSONObject> {
    public void reduce(Text key, Iterable<Text> value, Context context) {
        Iterator<Text> textIterator = value.iterator();
        ArrayList<String> arrayList = new ArrayList<String>();
        while (textIterator.hasNext())
            arrayList.add(textIterator.next().toString());
        String[] tweet = arrayList.toArray(new String[arrayList.size()]);
        BSONObject object = BasicDBObjectBuilder.start().add().add();

    }
}
