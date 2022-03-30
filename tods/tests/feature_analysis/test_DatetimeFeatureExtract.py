import unittest

from d3m import container, utils
from d3m.metadata import base as metadata_base

from tods.feature_analysis import DatetimeFeatureExtract


class DatetimeFeatureExtractTestCase(unittest.TestCase):
    def test_basic(self):
        self.maxDiff = None
        main = container.DataFrame(
            {
                "timestamp": [
                    "2021-02-17 13:46:24",
                    "2021-02-17 15:45:52",
                    "2021-02-17 16:02:56",
                    "2021-02-17 18:19:28",
                    "2021-02-17 23:01:04",
                    "2021-02-18 02:30:08",
                    "2021-02-18 03:42:40",
                    "2021-02-18 04:59:28",
                    "2021-02-18 05:25:04",
                    "2021-02-18 06:03:28",
                ]
            },
            columns=["timestamp"],
            generate_metadata=True,
        )

        self.assertEqual(
            utils.to_json_structure(main.metadata.to_internal_simple_structure()),
            [
                {
                    "selector": [],
                    "metadata": {
                        "schema": "https://metadata.datadrivendiscovery.org/schemas/v0/container.json",
                        "structural_type": "d3m.container.pandas.DataFrame",
                        "semantic_types": [
                            "https://metadata.datadrivendiscovery.org/types/Table"
                        ],
                        "dimension": {
                            "name": "rows",
                            "semantic_types": [
                                "https://metadata.datadrivendiscovery.org/types/TabularRow"
                            ],
                            "length": 10,
                        },
                    },
                },
                {
                    "selector": ["__ALL_ELEMENTS__"],
                    "metadata": {
                        "dimension": {
                            "name": "columns",
                            "semantic_types": [
                                "https://metadata.datadrivendiscovery.org/types/TabularColumn"
                            ],
                            "length": 1,
                        }
                    },
                },
                {
                    "selector": ["__ALL_ELEMENTS__", 0],
                    "metadata": {"structural_type": "str", "name": "timestamp"},
                },
            ],
        )
        hyperparams_class = (
            DatetimeFeatureExtract.DatetimeFeatureExtractPrimitive.metadata.get_hyperparams()
        )

        hp = hyperparams_class.defaults().replace(
            {
                "use_columns": [0],
                "use_semantic_types": False,
                # 'window_size':2
            }
        )

        primitive = DatetimeFeatureExtract.DatetimeFeatureExtractPrimitive(
            hyperparams=hp
        )

        output_main = primitive._produce(inputs=main).value
        print(output_main)
        expected_output = container.DataFrame(
            {
                "year": [2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021],
                "month": [2, 2, 2, 2, 2, 2, 2, 2, 2, 2],
                "day": [17, 17, 17, 17, 17, 18, 18, 18, 18, 18],
                "weekday": [2, 2, 2, 2, 2, 3, 3, 3, 3, 3],
                "hour": [13, 15, 16, 18, 23, 2, 3, 4, 5, 6],
            },
            columns=["year", "month", "day", "weekday", "hour"],
        )

        self.assertEqual(
            output_main[["year", "month", "day", "weekday", "hour"]].values.tolist(),
            expected_output[
                ["year", "month", "day", "weekday", "hour"]
            ].values.tolist(),
        )

        self.assertEqual(
            utils.to_json_structure(
                output_main.metadata.to_internal_simple_structure()
            ),
            [
                {
                    "selector": [],
                    "metadata": {
                        "schema": "https://metadata.datadrivendiscovery.org/schemas/v0/container.json",
                        "structural_type": "d3m.container.pandas.DataFrame",
                        "semantic_types": [
                            "https://metadata.datadrivendiscovery.org/types/Table"
                        ],
                        "dimension": {
                            "name": "rows",
                            "semantic_types": [
                                "https://metadata.datadrivendiscovery.org/types/TabularRow"
                            ],
                            "length": 10,
                        },
                    },
                },
                {
                    "selector": ["__ALL_ELEMENTS__"],
                    "metadata": {
                        "dimension": {
                            "name": "columns",
                            "semantic_types": [
                                "https://metadata.datadrivendiscovery.org/types/TabularColumn"
                            ],
                            "length": 6,
                        }
                    },
                },
                {
                    "selector": ["__ALL_ELEMENTS__", 0],
                    "metadata": {"structural_type": "str", "name": "timestamp"},
                },
                {
                    "selector": ["__ALL_ELEMENTS__", 1],
                    "metadata": {
                        "name": "year",
                        "structural_type": "numpy.int64",
                        "semantic_types": [
                            "https://metadata.datadrivendiscovery.org/types/Attribute"
                        ],
                    },
                },
                {
                    "selector": ["__ALL_ELEMENTS__", 2],
                    "metadata": {
                        "name": "month",
                        "structural_type": "numpy.int64",
                        "semantic_types": [
                            "https://metadata.datadrivendiscovery.org/types/Attribute"
                        ],
                    },
                },
                {
                    "selector": ["__ALL_ELEMENTS__", 3],
                    "metadata": {
                        "name": "day",
                        "structural_type": "numpy.int64",
                        "semantic_types": [
                            "https://metadata.datadrivendiscovery.org/types/Attribute"
                        ],
                    },
                },
                {
                    "selector": ["__ALL_ELEMENTS__", 4],
                    "metadata": {
                        "name": "weekday",
                        "structural_type": "numpy.int64",
                        "semantic_types": [
                            "https://metadata.datadrivendiscovery.org/types/Attribute"
                        ],
                    },
                },
                {
                    "selector": ["__ALL_ELEMENTS__", 5],
                    "metadata": {
                        "name": "hour",
                        "structural_type": "numpy.int64",
                        "semantic_types": [
                            "https://metadata.datadrivendiscovery.org/types/Attribute"
                        ],
                    },
                },
            ],
        )

        params = primitive.get_params()
        primitive.set_params(params=params)


if __name__ == "__main__":
    unittest.main()
