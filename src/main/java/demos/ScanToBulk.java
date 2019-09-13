package demos;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.*;

import java.io.IOException;
import java.util.Scanner;

final class ScanToBulk {

    private final Logger log = LogManager.getLogger(ScanToBulk.class);

    private final BulkRequest bulkRequest;

    private final String INDEX = "index";
    private final String DELETE = "delete";

    private void add(IndexRequest indexRequest) {
        bulkRequest.add(indexRequest);
    }

    final BulkRequest getBulkRequest() {
        return this.bulkRequest;
    }

    // Package a bulk import file in NDJSON format to a bulk request
    ScanToBulk(Scanner scanner, BulkRequest bulkRequest, Index index) throws IOException {
        this.bulkRequest = bulkRequest;

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

                // setup theIndexRequest
                IndexRequest indexRequest = new IndexRequest(index.lowerCaseString);

                // the next line contains the raw JSON, add to the bulk request
                String payload = scanner.nextLine();
                this.add(indexRequest
                        .source(payload, XContentType.JSON));
            } else if (parser.currentName().equals(DELETE)) {
                log.printf(Level.WARN, "Skipping unexpected delete bulk operation");
            } else {
                log.printf(Level.WARN, "Skipping unexpected %s bulk operation", parser.currentName());
                scanner.nextLine();
            }
        }
    }

}
