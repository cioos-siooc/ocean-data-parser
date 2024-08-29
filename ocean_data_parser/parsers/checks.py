import pandas as pd
from loguru import logger


def check_daylight_saving(time: pd.Series) -> bool:
    # Test daylight saving issue
    dt = time.diff()
    sampling_interval = dt.median()
    dst_fall = -pd.Timedelta("1h") + sampling_interval
    dst_spring = pd.Timedelta("1h") + sampling_interval
    has_issue = False
    if any(dt == dst_fall):
        logger.warning(
            (
                "Time gaps (=%s) for sampling interval of %s "
                "suggest a Fall daylight saving issue is present"
            ),
            dst_fall,
            sampling_interval,
        )
        has_issue = True

    if any(dt == dst_spring):
        logger.warning(
            (
                "Time gaps (=%s) for sampling interval of %s "
                "suggest a Spring daylight saving issue is present"
            ),
            dst_fall,
            sampling_interval,
        )
        has_issue = True
    return has_issue
