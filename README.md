For 2019 I dumped the fiestas into Json, incuding:
- Fiesta Nacional no sustituible
- Fiesta Nacional respecto de la que no se ha ejercido la facultad de sustitución
- Fiesta de Comunidad Autónoma

Source: https://www.boe.es/diario_boe/verifica.php?c=BOE-A-2018-14369&acc=Verificar&fix_bug_chrome=foo.pdf

please check vs PDF in case of error & remember to add any city/local fiestas

and then I indexed it into Elasticsearch see: https://github.com/elastic/elasticsearch

this application will create 2 x indices in your local Elasticsearch cluster

```fiestas_vs_comunidad_autonoma``` is a bulk import of https://github.com/djptek/fiestamapper/blob/master/src/main/resources/fiestas.js

```comunidad_autonoma_vs_fiestas``` refactors the data by denormalizing, joined
 by comunidad and adding [lon, lat] and isocode so that you can map the data using
  Kibana see: https://github.com/elastic/kibana

here are some sample queries Vs ```fiestas_vs_comunidad_autonoma```

```# How many days in the year have a fiesta (somewhere)?
   # (some days have more than one fiesta)
   GET fiestas_vs_comunidad_autonoma/_search
   {
     "size": 0,
     "aggs": {
       "count_dates": {
         "cardinality": {
           "field": "date"
           }
         }
       }
     }
   }
   
   # Fiesta Count by Community
   GET fiestas_vs_comunidad_autonoma/_search
   {
     "size": 0,
     "aggs": {
       "check_comunidades": {
         "terms": {
           "field": "comunidades",
           "size": 19,
           "order": {
             "_key": "asc"
           }
         }
       }
     }
   }
   
   # All Fiestas foreach Community
   # This could have gone in the pipeline
   # Note: hard-coding 19 is _not_ a political statement on my part, 
   # it's just the numnber of columns in the source data. 
   # If that worries you use a composite aggregation
   GET fiestas_vs_comunidad_autonoma/_search?filter_path=**.key
   {
     "size": 0,
     "aggs": {
       "my_comunidades": {
         "terms": {
           "field": "comunidades",
           "size": 19,
           "order": {
             "_key": "asc"
           }
         },
         "aggs": {
           "my_local_fiesta": {
             "terms": {
               "script": {
                 "source": "doc['date'].value.toLocalDate()+' - '+doc['fiesta.keyword'].value"
               },
               "size": 14
             }
           }
         }
       }
     }
   }
   
   # When is San José?
   GET fiestas_vs_comunidad_autonoma/_search?filter_path=**.date
   {
     "query": {
       "match_phrase": {
         "fiesta": "San José"
       }
     }
   }
   
   # Which Community celebrates San José?
   GET fiestas_vs_comunidad_autonoma/_search?filter_path=**.comunidades
   {
     "query": {
       "match_phrase": {
         "fiesta": "San José"
       }
     }
   }
   
   # How many days are Fiestas when Madrid doesn't have a fiesta?
   GET fiestas_vs_comunidad_autonoma/_search
   {
     "size": 0, 
     "query": {
       "bool": {
         "must_not": [
           {"match": {
             "comunidades": "MADRID"
           }}
         ]
       }
     },
     "aggs": {
       "remove_duplicate_dates": {
         "cardinality": {
           "field": "date"
         }
       }
     }
   }

   # Now let's aggregate on the geo_point field
   
   GET comunidad_autonoma_vs_fiestas/_search
   {
     "aggs": {
       "filter_agg": {
         "filter": {
           "geo_bounding_box": {
             "ignore_unmapped": true,
             "location_geo_point": {
               "top_left": {
                 "lat": 52.11025,
                 "lon": -24.86206
               },
               "bottom_right": {
                 "lat": 26.49397,
                 "lon": 16.66626
               }
             }
           }
         },
         "aggs": {
           "2": {
             "geohash_grid": {
               "field": "location_geo_point",
               "precision": 4
             },
             "aggs": {
               "3": {
                 "geo_centroid": {
                   "field": "location_geo_point"
                 }
               }
             }
           }
         }
       }
     },
     "size": 0
   }
   
   ```
