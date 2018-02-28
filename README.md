

execute example:
```bash
# result-type: percentage
>  ./check_elasticsearch_metrics.py -p 9200 \                                                                                                                                                                                            2 ↵
        --host log.int.mustapp.me \
        -c 15 -w 2 \
        -q "env:'production' AND program:'must.backend'" \
        --aggregation_name "mon-req-backend-api-warning-rate-percents" \
        --aggregation_type "significant_terms" \
        --aggregation_field "level.raw" \
        --aggregation_result_bucket_key "WARN" \
        --aggregation_result_type "percentage" \
        -s 600 \
        --index_prefix "logstash"
```

