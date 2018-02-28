import pytest
import sure

import argparse
import datetime

import check_elasticsearch_metrics


class TestBuildIndices:
    class StubDatetime:
        @staticmethod
        def now():
            return datetime.datetime(2018, 1, 15)

    @pytest.fixture(scope="class")
    def mock_datetime(self):
        check_elasticsearch_metrics.datetime = self.StubDatetime
        yield 1
        check_elasticsearch_metrics.datetime = datetime.datetime

    def test_base_case_positive(self, mock_datetime):
        index = check_elasticsearch_metrics.build_indices(indices_count=2, index_pattern="{prefix}-{yyyy}.{mm}.{dd}", index_prefix="logstash")

        index.should.be.equal("logstash-2018.01.15,logstash-2018.01.14")

    def test_defaults(self, mock_datetime):
        index = check_elasticsearch_metrics.build_indices()

        index.should.be.equal("logstash-2018.01.15,logstash-2018.01.14")

    def test_indices_count_eq_1(self):
        index = check_elasticsearch_metrics.build_indices(indices_count=1)
        index.should.be.equal("logstash-2018.01.15")

    def test_indices_count_gt_1(self):
        index = check_elasticsearch_metrics.build_indices(indices_count=2)
        index.should.be.equal("logstash-2018.01.15,logstash-2018.01.14")

        index = check_elasticsearch_metrics.build_indices(indices_count=3)
        index.should.be.equal("logstash-2018.01.15,logstash-2018.01.14,logstash-2018.01.13")

    def test_index_pattern_constant(self):
        index = check_elasticsearch_metrics.build_indices(indices_count=1, index_pattern="const", index_prefix="xxx")
        index.should.be.equal("const")

    def test_index_pattern_use_prefix(self):
        index = check_elasticsearch_metrics.build_indices(indices_count=1, index_pattern="pattern-{prefix}", index_prefix="xxx")
        index.should.be.equal("pattern-xxx")


# class TestParseArgs:
#     def test(self):
#         assert 0


class Stub(dict):
    MARKER = object()

    def __init__(self, value=None):
        if value is None:
            pass
        elif isinstance(value, dict):
            for key in value:
                self.__setitem__(key, value[key])
        else:
            raise TypeError('expected dict')

    def __setitem__(self, key, value):
        if isinstance(value, dict) and not isinstance(value, Stub):
            value = Stub(value)
        super(Stub, self).__setitem__(key, value)

    def __getitem__(self, key):
        found = self.get(key, Stub.MARKER)
        if found is Stub.MARKER:
            found = Stub()
            super(Stub, self).__setitem__(key, found)
        return found

    __setattr__, __getattr__ = __setitem__, __getitem__


class TestHandleElasticResponse:

    def test_no_aggregation(self):
        response = Stub({"hits": {"total": 500}})
        # response.hits.total = 500

        args = argparse.Namespace(aggregation_name=None, aggregation_type=None, aggregation_field=None)

        result = check_elasticsearch_metrics.handle_elastic_response(args, response)
        result.should.be.equal(500)

    @pytest.fixture(scope="class",
                    params=[
                        {
                            "args": {
                                "aggregation_result_bucket_key": ["WARN"],
                                "aggregation_result_type": "count"
                            },
                            "expected_result": 260
                        },
                        {
                            "args": {
                                "aggregation_result_bucket_key": ["WARN", "ERROR"],
                                "aggregation_result_type": "count"
                            },
                            "expected_result": 1320
                        },
                        {
                            "args": {
                                "aggregation_result_bucket_key": ["WARN"],
                                "aggregation_result_type": "percentage"
                            },
                            "expected_result": 12.0
                        },
                        {
                            "args": {
                                "aggregation_result_bucket_key": ["WARN", "ERROR"],
                                "aggregation_result_type": "percentage"
                            },
                            "expected_result": 60.94
                        }
                    ],
                    ids=["single result-bucket-key, count result-type",
                         "multi result-bucket-key, count result-type",
                         "single result-bucket-key, percentage result-type",
                         "multi result-bucket-key, percentage result-type", ])
    def aggregation_fixture(self, request):
        args = argparse.Namespace(aggregation_name="elastic-plugin-tests",
                                  aggregation_type="significant_terms",
                                  aggregation_field="level.raw",
                                  **request.param['args'])
        request.param["args"] = args

        response = Stub(
            {
                "aggregations": {
                    args.aggregation_name: {
                        "doc_count": 2166,
                        "bg_count": 2950965,
                        "buckets": [
                            Stub({
                                "key": "WARN",
                                "doc_count": 260,
                                "score": 10.642715055124022,
                                "bg_count": 14992
                            })
                            ,
                            Stub({
                                "key": "ERROR",
                                "doc_count": 1060,
                                "score": 10.642715055124022,
                                "bg_count": 14992
                            })
                            ,
                            Stub({
                                "key": "INFO",
                                "doc_count": 846,
                                "score": 3.289777164939534,
                                "bg_count": 425830
                            })
                        ]
                    }
                }
            })

        return response, request.param

    @pytest.fixture(scope="class",
                    params=[
                        {
                            "args": [
                                "--aggregation_result_bucket_key", "500..504",
                                "--aggregation_result_type", "count"
                            ],
                            "expected_result": ["500", "501", "502", "503", "504"]
                        },
                        {
                            "args": [
                                "--aggregation_result_bucket_key", "500",
                                "--aggregation_result_type", "percentage"
                            ],
                            "expected_result": ["500"]
                        },
                        {
                            "args": [
                                "--aggregation_result_bucket_key", "200",
                                "--aggregation_result_bucket_key", "500..504",
                                "--aggregation_result_type", "percentage"
                            ],
                            "expected_result": ["200", "500", "501", "502", "503", "504"]
                        }
                    ],
                    ids=["aggregation bucket key is range",
                         "aggregation bucket key single item",
                         "aggregation bucket key mix of range and single item",
                         ])
    def aggregation_range_bucket_key_fixture(self, request):
        # required args for script parsing
        args = ["--aggregation_name", "elastic-plugin-tests",
                "--aggregation_type", "significant_terms",
                "--aggregation_field", "response.keyword",
                "--critical", "10",
                "--warning", "5",
                "--host", "test.me",
                "--seconds", "600",
                "--query", "test"]
        args.extend(request.param['args'])

        request.param["args"] = args

        return request.param

    def test_has_aggregation_by_count(self, aggregation_fixture):
        response, req_params = aggregation_fixture
        args = req_params["args"]
        expected_result = req_params["expected_result"]

        result = check_elasticsearch_metrics.handle_elastic_response(args, response)
        result.should.be.equal(expected_result)

    def test_aggregation_result_bucket_key_special(self, aggregation_range_bucket_key_fixture):
        req_params = aggregation_range_bucket_key_fixture
        args = req_params["args"]
        expected_result = req_params["expected_result"]

        result = check_elasticsearch_metrics.parse_args(args)
        result.aggregation_result_bucket_key.should.be.equal(expected_result)


class TestGetAlertStatus:

    def test_value_lt_warning(self):
        args = argparse.Namespace(critical=15,
                                  warning=2,
                                  reverse=False
                                  )

        alert_status = check_elasticsearch_metrics.get_alert_status(args, value=0)
        alert_status.should.be.equal(check_elasticsearch_metrics.NagiosReturnCodes.OK.value)

    def test_value_equal_warning(self):
        args = argparse.Namespace(critical=15,
                                  warning=2,
                                  reverse=False
                                  )

        alert_status = check_elasticsearch_metrics.get_alert_status(args, value=2)
        alert_status.should.be.equal(check_elasticsearch_metrics.NagiosReturnCodes.WARNING.value)

    def test_value_gt_warning_and_lt_critical(self):
        args = argparse.Namespace(critical=15,
                                  warning=2,
                                  reverse=False
                                  )

        alert_status = check_elasticsearch_metrics.get_alert_status(args, value=3)
        alert_status.should.be.equal(check_elasticsearch_metrics.NagiosReturnCodes.WARNING.value)

    def test_value_eq_critical(self):
        args = argparse.Namespace(critical=15,
                                  warning=2,
                                  reverse=False
                                  )

        alert_status = check_elasticsearch_metrics.get_alert_status(args, value=15)
        alert_status.should.be.equal(check_elasticsearch_metrics.NagiosReturnCodes.CRITICAL.value)

    def test_value_gt_critical(self):
        args = argparse.Namespace(critical=15,
                                  warning=2,
                                  reverse=False
                                  )

        alert_status = check_elasticsearch_metrics.get_alert_status(args, value=16)
        alert_status.should.be.equal(check_elasticsearch_metrics.NagiosReturnCodes.CRITICAL.value)

    def test_value_gt_warning_reverse_true(self):
        args = argparse.Namespace(critical=2,
                                  warning=15,
                                  reverse=True
                                  )

        alert_status = check_elasticsearch_metrics.get_alert_status(args, value=16)
        alert_status.should.be.equal(check_elasticsearch_metrics.NagiosReturnCodes.OK.value)

    def test_value_eq_warning_reverse_true(self):
        args = argparse.Namespace(critical=2,
                                  warning=15,
                                  reverse=True
                                  )

        alert_status = check_elasticsearch_metrics.get_alert_status(args, value=15)
        alert_status.should.be.equal(check_elasticsearch_metrics.NagiosReturnCodes.WARNING.value)

    def test_value_lt_warning_and_gt_critical_reverse_true(self):
        args = argparse.Namespace(critical=2,
                                  warning=15,
                                  reverse=True
                                  )

        alert_status = check_elasticsearch_metrics.get_alert_status(args, value=10)
        alert_status.should.be.equal(check_elasticsearch_metrics.NagiosReturnCodes.WARNING.value)

    def test_value_eq_critical_reverse_true(self):
        args = argparse.Namespace(critical=2,
                                  warning=15,
                                  reverse=True
                                  )

        alert_status = check_elasticsearch_metrics.get_alert_status(args, value=2)
        alert_status.should.be.equal(check_elasticsearch_metrics.NagiosReturnCodes.CRITICAL.value)

    def test_value_lt_critical_reverse_true(self):
        args = argparse.Namespace(critical=2,
                                  warning=15,
                                  reverse=True
                                  )

        alert_status = check_elasticsearch_metrics.get_alert_status(args, value=1)
        alert_status.should.be.equal(check_elasticsearch_metrics.NagiosReturnCodes.CRITICAL.value)

    def test_decimal_values(self):
        args = argparse.Namespace(critical=15,
                                  warning=2,
                                  reverse=False
                                  )

        alert_status = check_elasticsearch_metrics.get_alert_status(args, value=1.9)
        alert_status.should.be.equal(check_elasticsearch_metrics.NagiosReturnCodes.OK.value)

        args = argparse.Namespace(critical=15,
                                  warning=2.0,
                                  reverse=False
                                  )

        alert_status = check_elasticsearch_metrics.get_alert_status(args, value=2)
        alert_status.should.be.equal(check_elasticsearch_metrics.NagiosReturnCodes.WARNING.value)

        args = argparse.Namespace(critical=15,
                                  warning=2,
                                  reverse=False
                                  )

        alert_status = check_elasticsearch_metrics.get_alert_status(args, value=2.0)
        alert_status.should.be.equal(check_elasticsearch_metrics.NagiosReturnCodes.WARNING.value)

        args = argparse.Namespace(critical=15,
                                  warning=2,
                                  reverse=False
                                  )

        alert_status = check_elasticsearch_metrics.get_alert_status(args, value=15.1)
        alert_status.should.be.equal(check_elasticsearch_metrics.NagiosReturnCodes.CRITICAL.value)

# class TestCheckExitCode:
#     def test(self):
#         assert 0
