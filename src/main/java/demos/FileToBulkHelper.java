package demos;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.*;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Scanner;

final class FileToBulkHelper {

    private final Logger log = LogManager.getLogger(FileToBulkHelper.class);
    private final String INDEX = "index";
    private final String INDEX_KEY = "_index";
    private final String ID_KEY = "_id";

    private final BulkRequest bulkRequest;
    private final File file;

    private void add(IndexRequest indexRequest) {
        bulkRequest.add(indexRequest);
    }

    final BulkRequest getBulkRequest() {
        return this.bulkRequest;
    }

    // Package a bulk import file in NDJSON format to a bulk request
    FileToBulkHelper(File file, BulkRequest bulkRequest, Index index) throws IOException {
        Scanner scanner = new Scanner(file).useDelimiter("\n");
        this.bulkRequest = bulkRequest;
        this.file = file;
        while (scanner.hasNext()) {
            String payload = scanner.nextLine();
            IndexRequest indexRequest = new IndexRequest(index.lowerCaseString);
            this.add(indexRequest.source(payload, XContentType.JSON));
        }
        scanner.close();
    }

    FileToBulkHelper(File file, BulkRequest bulkRequest) throws IOException {
        Scanner scanner = new Scanner(file).useDelimiter("\n");
        this.bulkRequest = bulkRequest;
        this.file = file;
        while (scanner.hasNext()) {
            String opLine = scanner.nextLine();
            XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                    .createParser(
                            NamedXContentRegistry.EMPTY,
                            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                            opLine);

            // this will be the "object" token
            parser.nextToken();

            // now advance to the operation type
            parser.nextToken();

            // only interested in index
            if (parser.currentName().equals(INDEX)) {

                parser.nextToken();

                // setup theIndexRequest
                Map<String, Object> requestMap = parser.map();
                IndexRequest indexRequest = new IndexRequest(requestMap.get(INDEX_KEY).toString())
                        .id(requestMap.get(ID_KEY).toString());
                // the next line contains the raw JSON, add to the bulk request
                String payload = scanner.nextLine();
                this.add(indexRequest
                        .source(payload, XContentType.JSON));
            } else {
                log.printf(Level.WARN, "Skipping unexpected %s bulk operation", parser.currentName());
                scanner.nextLine();
            }
        }
        scanner.close();
    }

    File getFile() {
        return file;
    }
}
