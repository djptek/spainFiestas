package demos;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.ParsedTopHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Map;


final class AggsToBulk {

    private final Logger log = LogManager.getLogger(AggsToBulk.class);

    private final BulkRequest bulkRequest;

    private void add(IndexRequest indexRequest) {
        bulkRequest.add(indexRequest);
    }

    // Max fiestas per comunidad currently 14, add some headroom
    private final static int MAX_FIESTAS = 100;

    final BulkRequest getBulkRequest() {
        return this.bulkRequest;
    }

    // Package a bulk import file in NDJSON format to a bulk request
    AggsToBulk(Index from, Index to, RestHighLevelClient client, BulkRequest bulkRequest) throws IOException {
        this.bulkRequest = bulkRequest;

        SearchRequest searchRequest = new SearchRequest()
                .indices(Index.FIESTAS_VS_COMUNIDAD_AUTONOMA.lowerCaseString)
                .source(new SearchSourceBuilder()
                        .aggregation(AggregationBuilders
                                .terms("my_comunidades")
                                .field("comunidades")
                                .size(Comunidad.MAX_COMUNIDADES)
                                .subAggregation(
                                        AggregationBuilders.topHits("my_local_fiestas")
                                                .size(MAX_FIESTAS))));
        /*
         * send Synchronous request - keep searchRequest as a separate Object in case we need to loop until
         * the documents are searchable
         */
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        Terms myComunidadesAggregation = searchResponse.getAggregations().get("my_comunidades");
        for (Terms.Bucket bucket : myComunidadesAggregation.getBuckets()) {
            Comunidad comunidad = Comunidad.valueOfNormalized(bucket.getKeyAsString());
            log.printf(Level.INFO, "Creating Entities for %s", comunidad);
            ParsedTopHits myFiestasAggregation = bucket.getAggregations().get("my_local_fiestas");
            for (SearchHit hit : myFiestasAggregation.getHits()) {
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                try {
                    this.add(new IndexRequest(
                            Index.COMUNIDAD_AUTONOMA_VS_FIESTAS.lowerCaseString)
                            .id(comunidad + ":" + sourceAsMap.get("date"))
                            .source(XContentFactory.jsonBuilder()
                                    .startObject()
                                    .field("comunidad", comunidad.userLevel)
                                    .field("location_geo_point", new double[]{
                                            comunidad.lon,
                                            comunidad.lat})
                                    .startObject("location_geo_shape")
                                    .field("type", "point")
                                    .field("coordinates", new double[]{
                                            comunidad.lon,
                                            comunidad.lat})
                                    .endObject()
                                    .field("isocode", comunidad.isocode)
                                    .startObject("fiesta")
                                    .field("name", sourceAsMap.get("fiesta"))
                                    .field("date", sourceAsMap.get("date"))
                                    .endObject()
                                    .endObject()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}



