package demos;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;

class BulkResponseDumper {

    private Logger log;

    BulkResponseDumper (Logger log) {
        this.log = log;
    }

    void dump(BulkResponse bulkResponse) {
        for (BulkItemResponse bulkItemResponse : bulkResponse.getItems()) {
        log.printf(Level.INFO, "Bulk Operation [%s] id [%s] to [%s] status [%s]",
                bulkItemResponse.getOpType(),
                bulkItemResponse.getId(),
                bulkItemResponse.getIndex(),
                bulkItemResponse.status());
        }
    }


}
