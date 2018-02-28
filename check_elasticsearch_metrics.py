#!/usr/bin/env python


import sys
import argparse
import logging
from datetime import datetime, timedelta

from elasticsearch import Elasticsearch, exceptions
from elasticsearch_dsl import Search, A

from enum import Enum

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

version = "0.1"


class NagiosReturnCodes(Enum):
    OK = 0
    WARNING = 1
    CRITICAL = 2
    UNKNOWN = 3


def parse_args(argv):
    arg_parser = argparse.ArgumentParser(description="Obtains metrics from elasticsearch to power Icinga alerts",
                                         formatter_class=argparse.RawDescriptionHelpFormatter,
                                         epilog="Error codes:\n"
                                                "\t0: Everything OK, check passed\n"
                                                "\t1: Warning threshold breached\n"
                                                "\t2: Critical threshold breached\n"
                                                "\t3: Unknown, encountered an error querying elasticsearch\n")

    # required options
    arg_parser.add_argument("-c", "--critical", action="store", type=float, required=True, help="critical threshold")
    arg_parser.add_argument("--host", action="store", default="localhost", required=True, help="elasticsearch host (default: localhost)")
    arg_parser.add_argument("-i", "--indices_count", action="store", type=int, default=2,
                            help="the number of indices to go back through (default: 2)")
    arg_parser.add_argument("--index_prefix", action="store", default="logstash", help="index prefix (default: logstash")
    arg_parser.add_argument("-n", "--index_pattern", action="store", default="{prefix}-{yyyy}.{mm}.{dd}",
                            help="the pattern expects months and years and can take a prefix and days, e.g: metrics-{yyyy}.{mm}")
    arg_parser.add_argument("-s", "--seconds", action="store", type=int, required=True, help="number of seconds from now to check")
    arg_parser.add_argument("-q", "--query", action="store", required=True, help="the query to run in elasticsearch")
    arg_parser.add_argument("-w", "--warning", action="store", type=float, required=True, help="warning threshold")

    # optional options
    arg_parser.add_argument("--aggregation_name", action="store", help="aggregation name")
    arg_parser.add_argument("--aggregation_type", action="store", choices=("significant_terms",), help="aggregation type")
    arg_parser.add_argument("--aggregation_field", action="store", help="the name of the field to aggregate")
    arg_parser.add_argument("--aggregation_result_bucket_key", action="append",  help="specify aggregation bucket keys (repeatable argument)")
    arg_parser.add_argument("--aggregation_result_type", action="store", choices=("count", "percentage"), default="count",
                            help="aggregation result type (default: count)")
    arg_parser.add_argument("-d", "--include_day", action="store_true", help="include the day in elasticsearch index")
    arg_parser.add_argument("-p", "--port", action="store", type=int, default=9200, help="elasticsearch port (default: 9200)")
    arg_parser.add_argument("-r", "--reverse", action="store_true", help="reverse threshold (so amounts below threshold values will alert)")
    arg_parser.add_argument("--debug", action="store_true", default=False, help="print debug messages")
    arg_parser.add_argument("--version", action="version", version='%(prog)s {version}'.format(version=version))

    args = arg_parser.parse_args(argv)

    def flat_bucket_keys(args):
        # handle range specific items (for example: `aggregation_result_bucket_key=500..504`)
        if args.aggregation_result_bucket_key:
            for bucket in list(filter(lambda x: x.find("..") > 0, args.aggregation_result_bucket_key)):
                args.aggregation_result_bucket_key.remove(bucket)

                range_start, range_finish = list(map(int, bucket.split("..")))
                args.aggregation_result_bucket_key.extend(list(map(str, list(range(range_start, range_finish+1)))))

    flat_bucket_keys(args)

    return args


def build_indices(indices_count=2, index_pattern="{prefix}-{yyyy}.{mm}.{dd}", index_prefix="logstash"):
    indices = []
    today = datetime.now().date()

    for i in range(indices_count):
        t = today - timedelta(days=i)
        indices.append(index_pattern.format(prefix=index_prefix,
                                            yyyy=t.year,
                                            mm="{:02d}".format(t.month),
                                            dd="{:02d}".format(t.day)))

    return ",".join(indices)


def calc_percent(part, whole):
    if whole <= int(0):
        return 0

    return round(part * 100 / whole, 2)


def get_alert_status(args, value):
    if args.reverse:
        if value <= args.critical:
            return NagiosReturnCodes.CRITICAL.value
        if value <= args.warning:
            return NagiosReturnCodes.WARNING.value
    else:
        if value >= args.critical:
            return NagiosReturnCodes.CRITICAL.value
        if value >= args.warning:
            return NagiosReturnCodes.WARNING.value

    return NagiosReturnCodes.OK.value


def need_aggregate(args):
    return args.aggregation_name and args.aggregation_type and args.aggregation_field


def execute_elastic_query(args):
    logger.debug(args)

    query = args.query
    host = args.host
    port = args.port
    from_time = "now-{seconds}s".format(seconds=args.seconds)
    aggregate = need_aggregate(args)

    index = build_indices(indices_count=args.indices_count,
                          index_pattern=args.index_pattern,
                          index_prefix=args.index_prefix)

    client = Elasticsearch(hosts=["{}:{}".format(host, port)])

    s = Search(using=client, index=index) \
        .query("query_string", query=query, analyze_wildcard=True) \
        .query("range", **{"@timestamp": {"gte": "{}".format(from_time)}})

    if aggregate:
        s.aggs.bucket(args.aggregation_name, A(args.aggregation_type, field=args.aggregation_field))

    return s.execute()


def handle_elastic_response(args, response):
    result = 0

    if need_aggregate(args):
        res_aggregation = {}

        for bucket in response.aggregations[args.aggregation_name].buckets:
            res_aggregation.update({bucket.key:
                                        {"count": bucket.doc_count,
                                         "percentage": calc_percent(bucket.doc_count,
                                                                    response.aggregations[args.aggregation_name].doc_count)}
                                    })
        logger.debug(res_aggregation)

        # does not metter result_type is percentage or count
        for field in args.aggregation_result_bucket_key:
            result += res_aggregation.get(field).get(args.aggregation_result_type) if res_aggregation.get(field) else 0
    else:
        result = response.hits.total

    return result


def main(argv):
    args = parse_args(argv)

    if args.debug:
        logging.getLogger().setLevel(level=logging.DEBUG)

    result = 0
    try:
        logger.debug("args: {}".format(args))

        response = execute_elastic_query(args)
        result = handle_elastic_response(args, response)
    except exceptions.ElasticsearchException as e:
        logger.error("Got elasticsearch exception: {}".format(e))
        exit(NagiosReturnCodes.UNKNOWN.value)

    logger.debug("result: {}".format(result))

    alert_status = get_alert_status(args, result)
    logger.info("Exited with: {alert_status}, Current Value: {value}, Critical: {critical}, Warning: {warning}"
                .format(alert_status=alert_status, value=result, critical=args.critical, warning=args.warning))
    exit(alert_status)


if __name__ == '__main__':
    main(sys.argv[1:])
