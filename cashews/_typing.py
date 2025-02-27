from datetime import timedelta
from typing import Any, Callable, Dict, Tuple, Union

TTL = Union[int, str, timedelta, Callable[[], Union[int, timedelta]]]
CacheCondition = Union[Callable[[Any, Tuple, Dict], bool], str, None]
