#!/usr/bin/env python3

import logging
import apache_beam as beam
import datetime
import decimal
import json


class LogSample(beam.PTransform):

    """Print items of the given `PCollection` to the log.

    `LogSample` prints the JSON representations of the input items
    to the Python's standard logging system.

    To avoid too much log entries being printed,
    `LogSample` limits the number of logged items.
    The constructor parameter `n` determines the limit.

    By default, `LogSample` prints logs with the `INFO` log level.
    The constructor parameter `level` determines the level.
    """

    def __init__(self, n, level=logging.INFO):
        super().__init__()
        self._n = n
        self._level = level

    def expand(self, pcoll):
        def serialize(obj):
            if isinstance(obj, (datetime.date, datetime.datetime)):
                return obj.isoformat()
            if isinstance(obj, decimal.Decimal):
                return float(obj)
            raise TypeError(f"Type {type(obj)} is not serializable")

        def log(rows):
            logger = logging.getLogger(__name__)
            for r in rows:
                logger.log(
                    self._level,
                    json.dumps(r, default=serialize, ensure_ascii=False, indent=2),
                )

        return (
            pcoll
            | beam.transforms.combiners.Sample.FixedSizeGlobally(self._n)
            | beam.ParDo(log)
        )


@beam.ptransform_fn
def ReifyMultiValueOption(pcoll, option, delimiter="|"):
    """
    Prepares multi-value delimited options

    inputs:
      - delimited string option
      - optional delimiter string (default is "|")

    output:
      - PCollection of strings

    """
    return (
        pcoll
        | beam.Create([""])
        | beam.FlatMap(lambda _: option.get().split(delimiter))
    )


class MemoizedValueProviderWrapper:
    """
    Wrapper class to memoize parse result of value providers.
    Parsing function should only be called once, and result used many times

    eg. within a ParDo
    """

    def __init__(self, provider, func):
        self._value = None
        self._provider = provider
        self._func = func

    def get(self):
        if self._value is None:
            self._value = self._func(self._provider.get())
        return self._value
