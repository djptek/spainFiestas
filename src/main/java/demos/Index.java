package demos;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

import static org.elasticsearch.client.RequestOptions.DEFAULT;

enum Index {
    FIESTAS_VS_COMUNIDAD_AUTONOMA("fiestas_vs_comunidad_autonoma",
            "{\"number_of_shards\":1,\"number_of_replicas\":0,\"default_pipeline\":\"fiesta_to_id\"}",
            "{\"properties\":{\"comunidades\":"
                    + "{\"type\":\"keyword\"},\"date\":{\"type\":\"date\"},\"fiesta\":{\"type\":\"text\",\"fields\":"
                    + "{\"keyword\":{\"type\":\"keyword\"}}}}}"),
    COMUNIDAD_AUTONOMA_VS_FIESTAS("comunidad_autonoma_vs_fiestas",
            "{\"number_of_shards\":1,\"number_of_replicas\":0}",
            "{\"properties\":{\"comunidad\":"
                    + "{\"type\":\"keyword\"},\"fiesta\":{\"properties\":{\"date\":{\"type\":\"date\"},\"name\":{\"type\":"
                    + "\"keyword\"}}},\"isocode\":{\"type\":\"keyword\"},\"location_geo_point\":{\"type\":\"geo_point\"},"
                    + "\"location_geo_shape\":{\"type\":\"geo_shape\"}}}"),
    MUNICIPIO_VS_FIESTAS("municipio_vs_fiestas",
            "{\"number_of_shards\":1,\"number_of_replicas\":0}",
            "{\"properties\":{"
                        + "\"provincia\":{\"type\":\"keyword\"},"
                        + "\"municipio\":{\"properties\":{"
                            + "\"name\":{\"type\":\"keyword\"},"
                            + "\"location_geo_point\":{\"type\":\"geo_point\"}}},"
                        + "\"fiesta\":{\"properties\":{"
                            + "\"date\":{\"type\":\"date\"},"
                            + "\"name\":{\"type\":\"keyword\"}}}}}"),
    MUNICIPIO_VS_COORDS("municipio_vs_coords",
            "{\"number_of_shards\":1,\"number_of_replicas\":0}",
            "{}");


    public String lowerCaseString;
    public String settings;
    public String mapping;

    Index(String lowerCaseString, String settings, String mapping) {
        this.lowerCaseString = lowerCaseString;
        this.settings = settings;
        this.mapping = mapping;
    }

    static boolean exists(Index index, RestHighLevelClient client) throws IOException {
        return client.indices()
                .exists(new GetIndexRequest(index.lowerCaseString),
                        RequestOptions.DEFAULT);
    }

    static void delete(Index index, RestHighLevelClient client) throws IOException {
        client.indices()
                .delete(new DeleteIndexRequest(index.lowerCaseString),
                        DEFAULT);
    }

    static boolean create(Index index, RestHighLevelClient client) throws IOException {
        CreateIndexResponse response = client.indices()
                .create(new CreateIndexRequest(index.lowerCaseString)
                                .settings(index.settings,
                                        XContentType.JSON)
                                .mapping(index.mapping,
                                        XContentType.JSON),
                        RequestOptions.DEFAULT);
        return response.isAcknowledged();
    }

}