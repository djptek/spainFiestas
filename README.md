For 2019 I translated the fiestas into Json, incuding:

- Fiesta Nacional no sustituible
- Fiesta Nacional respecto de la que no se ha ejercido la facultad de sustitución
- Fiesta de Comunidad Autónoma

Source: https://www.boe.es/diario_boe/verifica.php?c=BOE-A-2018-14369&acc=Verificar&fix_bug_chrome=foo.pdf

Please check vs PDF in case of error & **PRs welcome to add any city/local fiestas**. Please include a URL verifying the local fiesta and a commit note with the coordinates of the fiesta, e.g. `ANDALUCIA("ANDALUCÍA", "ES-AN", -5.98, 37.37)`

```
usage: spain.fiestas*
 -H,--host <arg>       Host default <localhost>
 -p,--password <arg>   password default <password>
 -P,--port <arg>       Port default <9200>
 -S,--scheme <arg>     Scheme [http|https] default <http>
 -u,--username <arg>   username default <elastic>
``

To-do: 
- [ ] Tidy up command line params
- [ ] Move Community coordinates into separate resource file
- [ ] add Cities 
- [ ] Define Capitals <= Cities 

Then I indexed it into Elasticsearch see: https://github.com/elastic/elasticsearch

This application will create a pipeline + 2 x indices with pre-defined mappings in your local Elasticsearch cluster

```fiestas_vs_comunidad_autonoma``` is a bulk import of https://github.com/djptek/spainFiestas/blob/master/src/main/resources/fiestas.js

```comunidad_autonoma_vs_fiestas``` refactors the data by denormalizing, joined
 by comunidad and adding [lon, lat] and isocode so that you can map the data using
  Kibana see: https://github.com/elastic/kibana

Here are some sample queries Vs ```fiestas_vs_comunidad_autonoma```

```
# How many days in the year have a fiesta (somewhere)?
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
           "size": 100
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
```

Here is a sample aggregation Vs ```comunidad_autonoma_vs_fiestas```

```
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
