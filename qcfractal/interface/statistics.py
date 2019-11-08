"""A module for statistical quantities.
"""
import numpy as np
import pandas as pd

# Need to use specifically with the fitting_database class


def signed_error(value, bench, **kwargs):
    return value - bench


def unsigned_error(value, bench, **kwargs):
    return np.abs(value - bench)


def mean_signed_error(value, bench, **kwargs):
    return np.mean(value - bench)


def mean_unsigned_error(value, bench, **kwargs):
    return np.mean(np.abs(value - bench))


def unsigned_relative_error(value, bench, **kwargs):
    min_div = kwargs.get("floor", None)
    divisor = bench.copy()
    if min_div:
        divisor[np.abs(divisor) < min_div] = np.abs(min_div)
    return np.abs((value - bench) / divisor) * 100


def mean_unsigned_relative_error(value, bench, **kwargs):
    ure = unsigned_relative_error(value, bench, **kwargs)
    return np.mean(ure)


# Stats wrapped in a dictionary:
_stats_dict = {}
_stats_dict["E"] = signed_error
_stats_dict["ME"] = mean_signed_error
_stats_dict["UE"] = unsigned_error
_stats_dict["MUE"] = mean_unsigned_error
_stats_dict["URE"] = unsigned_relative_error
_stats_dict["MURE"] = mean_unsigned_relative_error

_return_series = ["ME", "MUE", "MURE", "WMURE"]


def wrap_statistics(description, ds, value, bench, **kwargs):
    # Get benchmark
    if isinstance(bench, str):
        rbench = ds.get_values(name=bench)[bench]
    elif isinstance(bench, (np.ndarray, pd.Series)):
        if len(bench.shape) != 1:
            raise ValueError("Only 1D numpy arrays can be passed to statistical quantities.")
        rbench = bench
    else:
        raise TypeError("Benchmark must be a column of the dataframe or a 1D numpy array.")

    if isinstance(value, str):
        rvalue = ds.get_values(name=value)[value]
        return _stats_dict[description](rvalue, rbench, **kwargs)

    elif isinstance(value, pd.Series):
        rvalue = value
        return _stats_dict[description](rvalue, rbench, **kwargs)

    elif isinstance(value, pd.DataFrame):
        return value.apply(lambda x: _stats_dict[description](x, rbench, **kwargs))

    elif isinstance(value, (list, tuple)):
        if description in _return_series:
            ret = pd.Series(index=value)
        else:
            ret = pd.DataFrame(columns=value)

        method = _stats_dict[description]
        for col in value:
            ret[col] = method(ds.get_values(name=col), rbench, **kwargs)
        return ret

    else:
        raise TypeError("Type {} is not understood for statistical quantities".format(str(type(value))))
