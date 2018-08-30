"""A module for statistical quantities.
"""
import numpy as np
import pandas as pd

# Need to use specifically with the fitting_database class

# def signed_error(value, bench):
#     return value - bench

# def unsigned_error(value, bench):
#     return np.abs(value - bench)


def mean_signed_error(value, bench):
    return np.mean(value - bench)


def mean_unsigned_error(value, bench):
    return np.mean(np.abs(value - bench))


# def unsigned_relative_error(value, bench):
#     return np.abs((value - bench) / bench) * 100


def mean_unsigned_relative_error(value, bench):
    return np.mean(np.abs((value - bench) / bench)) * 100


# def weighted_unsigned_relative_error(value, bench, weight):
#     return np.abs((value - bench) / weight) * 100

# def weighted_mean_unsigned_relative_error(value, bench, weight):
#     return np.mean(np.abs((value - bench) / weight)) * 100

# Stats wrapped in a dictionary:
_stats_dict = {}
# _stats_dict['E'] = signed_error
_stats_dict['ME'] = mean_signed_error
# _stats_dict['UE'] = unsigned_error
_stats_dict['MUE'] = mean_unsigned_error
# _stats_dict['URE'] = unsigned_relative_error
_stats_dict['MURE'] = mean_unsigned_relative_error
# _stats_dict['WURE'] = weighted_unsigned_relative_error
# _stats_dict['WMURE'] = weighted_mean_unsigned_relative_error

_return_series = ['ME', 'MUE', 'MURE', 'WMURE']

# _needs_weight = ["WURE", "WMURE"]


def wrap_statistics(description, df, value, bench):

    # Get benchmark
    if isinstance(bench, str):
        rbench = df[bench]
    elif isinstance(bench, (np.ndarray, pd.Series)):
        if len(bench.shape) != 1:
            raise ValueError('Only 1D numpy arrays can be passed to statistical quantities.')
        rbench = bench
    else:
        raise TypeError('Benchmark must a column of the dataframe or a 1D numpy array.')

    if isinstance(value, str):
        rvalue = df[value]
        return _stats_dict[description](rvalue, rbench)

    elif isinstance(value, pd.Series):
        rvalue = value
        return _stats_dict[description](rvalue, rbench)

    elif isinstance(value, pd.DataFrame):
        return value.apply(lambda x: _stats_dict[description](x, rbench))

    elif isinstance(value, (list, tuple)):
        if description in _return_series:
            ret = pd.Series(index=value)
        else:
            ret = pd.DataFrame(columns=value)

        method = _stats_dict[description]
        for col in value:
            ret[col] = method(df[col], rbench)
        return ret

    else:
        raise TypeError('Type {} is not understood for statistical quantities'.format(str(type(value))))
