package demos;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.File;
import java.io.IOException;

class KibanaObjectsLoader {
    private static Logger logger = LogManager.getLogger(App.class);
    private static String[] filenames = {
            "src/main/resources/.kibana_1_bulk.ndjson",
            "src/main/resources/.kibana_2_bulk.ndjson"};
    private static BulkResponseDumper bulkResponseDumper = new BulkResponseDumper(logger);
    private static BulkResponse bulkResponse;
    private static DataLoader dataLoader;
    private static RestHighLevelClient client;

    KibanaObjectsLoader(RestHighLevelClient client) {
        this.client = client;
    }

    static void load() throws IOException {
        for (String filename : filenames) {
            logger.log(Level.INFO, "Loading Kibana Objects "+filename);
            File file = new File(filename);
            dataLoader = new DataLoader(client, file, logger);
            bulkResponse =  dataLoader.load();
            bulkResponseDumper.dump(bulkResponse);
        }
    }

}
