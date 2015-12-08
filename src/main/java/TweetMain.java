import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.bson.BSONObject;
import org.bson.types.ObjectId;

import java.io.IOException;

import static org.apache.hadoop.util.ReflectionUtils.setConf;

/**
 * Created by marwen on 08/12/15.
 */
public class TweetMain implements Tool {

    public TweetMain() {
        Configuration configuration = new Configuration();
        MongoConfigUtil.setOutputFormat(configuration, MongoOutputFormat.class);
        MongoConfigUtil.setInputFormat(configuration, MongoInputFormat.class);
        MongoConfigUtil.setInputURI(configuration, "mongodb://localhost/hadoop.commonwords");
        MongoConfigUtil.setQuery(configuration, "{}");
        MongoConfigUtil.setOutputURI(configuration, "mongodb://localhost/hadoop.tweets");
        MongoConfigUtil.setMapper(configuration, TweetMap.class);
        MongoConfigUtil.setReducer(configuration, TweetReduce.class);
        MongoConfigUtil.setMapperOutputKey(configuration, Text.class);
        MongoConfigUtil.setMapperOutputValue(configuration, Text.class);
        MongoConfigUtil.setOutputKey(configuration, ObjectId.class);
        MongoConfigUtil.setOutputValue(configuration, BSONObject.class);

        setConf(configuration);

    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new TweetMain(),args));
    }

    public int run(String[] strings) throws Exception {
        return 0;
    }

    public void setConf(Configuration configuration) {

    }

    public Configuration getConf() {
        return null;
    }
}


