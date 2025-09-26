from datetime import datetime, time, timezone, timedelta
from fhir4ds.ast.nodes import DateTimeLiteral, TimeLiteral, DateTimePrecision, TimePrecision
import re
from typing import Optional

class DateTimeParser:
    """Parser for FHIRPath DateTime/Time literals"""

    # ISO 8601 datetime pattern
    DATETIME_PATTERN = re.compile(
        r'@(?P<year>\d{4})'
        r'(?:-(?P<month>\d{2}))?'
        r'(?:-(?P<day>\d{2}))?'
        r'(?:T(?P<hour>\d{2})'
        r'(?::(?P<minute>\d{2}))?'
        r'(?::(?P<second>\d{2}))?'
        r'(?:\.(?P<millisecond>\d{1,3}))?'
        r'(?P<timezone>Z|[+-]\d{2}:\d{2})?)?'
    )

    # Time-only pattern
    TIME_PATTERN = re.compile(
        r'@T(?P<hour>\d{2})'
        r'(?::(?P<minute>\d{2}))?'
        r'(?::(?P<second>\d{2}))?'
        r'(?:\.(?P<millisecond>\d{1,3}))?'
    )

    def parse_datetime(self, literal: str) -> DateTimeLiteral:
        """Parse datetime literal into DateTimeLiteral AST node"""
        match = self.DATETIME_PATTERN.match(literal)
        if not match:
            raise ValueError(f"Invalid DateTime literal format: {literal}")

        parts = match.groupdict()

        year = int(parts['year'])
        month = int(parts['month']) if parts['month'] else 1
        day = int(parts['day']) if parts['day'] else 1
        hour = int(parts['hour']) if parts['hour'] else 0
        minute = int(parts['minute']) if parts['minute'] else 0
        second = int(parts['second']) if parts['second'] else 0
        millisecond = int(parts['millisecond'].ljust(3, '0')) if parts['millisecond'] else 0
        microsecond = millisecond * 1000

        tz: Optional[timezone] = None
        if parts['timezone']:
            if parts['timezone'] == 'Z':
                tz = timezone.utc
            else:
                sign = 1 if parts['timezone'][0] == '+' else -1
                tz_hours = int(parts['timezone'][1:3])
                tz_minutes = int(parts['timezone'][4:6])
                tz = timezone(timedelta(hours=sign * tz_hours, minutes=sign * tz_minutes))

        dt_value = datetime(year, month, day, hour, minute, second, microsecond, tzinfo=tz)

        precision = DateTimePrecision.YEAR
        if parts['month']:
            precision = DateTimePrecision.MONTH
        if parts['day']:
            precision = DateTimePrecision.DAY
        if parts['hour']:
            precision = DateTimePrecision.HOUR
        if parts['minute']:
            precision = DateTimePrecision.MINUTE
        if parts['second']:
            precision = DateTimePrecision.SECOND
        if parts['millisecond']:
            precision = DateTimePrecision.MILLISECOND

        return DateTimeLiteral(value=dt_value, precision=precision, timezone=tz, source_location=None, metadata=None)

    def parse_time(self, literal: str) -> TimeLiteral:
        """Parse time literal into TimeLiteral AST node"""
        match = self.TIME_PATTERN.match(literal)
        if not match:
            raise ValueError(f"Invalid Time literal format: {literal}")

        parts = match.groupdict()

        hour = int(parts['hour'])
        minute = int(parts['minute']) if parts['minute'] else 0
        second = int(parts['second']) if parts['second'] else 0
        millisecond = int(parts['millisecond'].ljust(3, '0')) if parts['millisecond'] else 0
        microsecond = millisecond * 1000

        time_value = time(hour, minute, second, microsecond)

        precision = TimePrecision.HOUR
        if parts['minute']:
            precision = TimePrecision.MINUTE
        if parts['second']:
            precision = TimePrecision.SECOND
        if parts['millisecond']:
            precision = TimePrecision.MILLISECOND

        return TimeLiteral(value=time_value, precision=precision, source_location=None, metadata=None)