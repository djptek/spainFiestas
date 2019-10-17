package demos;

import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

enum ScriptPipeline {

    FIESTA_TO_ID("fiesta_to_id",
            "set _id to fiesta",
            "ctx._id = ctx.fiesta;");

    public String id;
    public String description;
    public String source;

    ScriptPipeline(String id, String description, String source) {
        this.id = id;
        this.description = description;
        this.source = source;
    }

    public static Boolean put(ScriptPipeline scriptPipeline,
                              RestHighLevelClient client) throws IOException {
        AcknowledgedResponse response = client.ingest()
                .putPipeline(new PutPipelineRequest(
                                scriptPipeline.id,
                                BytesReference
                                        .bytes(jsonBuilder()
                                                .startObject()
                                                .field("description", scriptPipeline.description)
                                                .startArray("processors")
                                                .startObject()
                                                .startObject("script")
                                                .field("source", scriptPipeline.source)
                                                .endObject()
                                                .endObject()
                                                .endArray()
                                                .endObject()),
                                XContentType.JSON),
                        RequestOptions.DEFAULT);
        return response.isAcknowledged();
    }

}
