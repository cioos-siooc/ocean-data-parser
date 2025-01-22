import pandas as pd
from loguru import logger
from pytz.exceptions import AmbiguousTimeError


def check_daylight_saving(
    time: pd.Series, ambiguous: str = "raise", fix_log_level: str = "info"
):
    """Check if daylight saving issue is present in the time series.

    Args:
        time (pd.Series): time series
        ambiguous (str, optional): Similar to pandas.Series.tz_localize.
            options:
                - "raise": raise when we encounter ambiguous dates (default)
                - else: warn when we encounter ambiguous dates.
        fix_log_level (str, optional): Log level to use when fixing the issue.

    Returns:
        bool: True if daylight saving issue is present
        pd.Series: time series with daylight saving issue fixed
    """
    # Test daylight saving issue
    dt = time.diff()
    sampling_interval = dt.median()
    dst_fall = -pd.Timedelta("1h") + sampling_interval
    dst_spring = pd.Timedelta("1h") + sampling_interval

    error_message = []
    if any(dt == dst_fall):
        error_message += [
            f"Time gaps (={dst_fall}) for sampling interval of {sampling_interval} "
            "suggest a Fall daylight saving issue is present"
        ]
    if any(dt == dst_spring):
        error_message += [
            f"Time gaps (={dst_spring}) for sampling interval of {sampling_interval} "
            "suggest a Spring daylight saving issue is present"
        ]

    # Handle errors based on ambiguous
    if not error_message:
        pass
    elif ambiguous == "raise" and error_message:
        error_message += [
            "To fix this issue, set ambiguous='warn' or provide "
            "a local timezone (e.g. 'Canada/Pacific')"
        ]
        raise AmbiguousTimeError("\n".join(error_message))
    else:
        logger.warning("\n".join(error_message))
