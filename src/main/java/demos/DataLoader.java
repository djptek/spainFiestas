package demos;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

interface DataLoader {
    Logger log = LogManager.getLogger(App.class);
    BulkResponseDumper bulkResponseDumper = new BulkResponseDumper(log);

    void load(RestHighLevelClient client, File file) throws IOException;

}

