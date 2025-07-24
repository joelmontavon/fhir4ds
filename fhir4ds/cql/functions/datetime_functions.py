"""
CQL Date/Time Functions

Implements comprehensive CQL date/time operations including component extraction,
duration calculations, temporal arithmetic, and enhanced date/time constructors.

Addresses Phase 4.2 critical gap - achieving 80%+ date/time compliance (currently 30%).
"""

import logging
from typing import Any, List, Dict, Union, Optional

logger = logging.getLogger(__name__)


class CQLDateTimeFunctionHandler:
    """
    CQL date/time function handler providing comprehensive temporal operations.
    
    Implements missing CQL date/time functions:
    - Component extraction (year from, month from, day from, etc.)
    - Duration calculations (years between, months between, etc.)
    - Difference calculations (difference in years between, etc.)
    - Temporal constructors (DateTime, Date, Time)
    - Temporal arithmetic and comparisons
    """
    
    def __init__(self, dialect: str = "duckdb"):
        """Initialize CQL date/time function handler with dialect support."""
        self.dialect = dialect
        
        # Register all date/time functions
        self.function_map = {
            # Component extraction functions
            'year': self.year_from,
            'month': self.month_from,
            'day': self.day_from,
            'hour': self.hour_from,
            'minute': self.minute_from,
            'second': self.second_from,
            'date': self.date_from,
            'time': self.time_from,
            
            # Duration calculation functions
            'years_between': self.years_between,
            'months_between': self.months_between,
            'days_between': self.days_between,
            'hours_between': self.hours_between,
            'minutes_between': self.minutes_between,
            'seconds_between': self.seconds_between,
            
            # Difference calculation functions (boundary crossings)
            'difference_in_years': self.difference_in_years,
            'difference_in_months': self.difference_in_months,
            'difference_in_days': self.difference_in_days,
            'difference_in_hours': self.difference_in_hours,
            'difference_in_minutes': self.difference_in_minutes,
            'difference_in_seconds': self.difference_in_seconds,
            
            # Temporal constructors
            'datetime': self.datetime_constructor,
            'date': self.date_constructor,
            'time': self.time_constructor,
            
            # Current date/time functions (enhance existing)
            'now': self.now,
            'today': self.today,
            'timeofday': self.time_of_day,
            
            # Temporal arithmetic (enhance existing predecessor/successor)
            'add_years': self.add_years,
            'add_months': self.add_months,
            'add_days': self.add_days,
            'add_hours': self.add_hours,
            'add_minutes': self.add_minutes,
            'add_seconds': self.add_seconds,
            
            # Temporal boundaries
            'start_of_year': self.start_of_year,
            'end_of_year': self.end_of_year,
            'start_of_month': self.start_of_month,
            'end_of_month': self.end_of_month,
            'start_of_day': self.start_of_day,
            'end_of_day': self.end_of_day,
        }
    
    # Component Extraction Functions
    
    def year_from(self, datetime_expr: Any) -> str:
        """
        CQL 'year from' operator - extract year component from date/datetime.
        
        Example: year from @2023-12-25T10:30:00 → 2023
        """
        logger.debug("Generating CQL year from operation")
        return f"EXTRACT(YEAR FROM CAST({datetime_expr} AS TIMESTAMP))"
    
    def month_from(self, datetime_expr: Any) -> str:
        """
        CQL 'month from' operator - extract month component from date/datetime.
        
        Example: month from @2023-12-25T10:30:00 → 12
        """
        logger.debug("Generating CQL month from operation")
        return f"EXTRACT(MONTH FROM CAST({datetime_expr} AS TIMESTAMP))"
    
    def day_from(self, datetime_expr: Any) -> str:
        """
        CQL 'day from' operator - extract day component from date/datetime.
        
        Example: day from @2023-12-25T10:30:00 → 25
        """
        logger.debug("Generating CQL day from operation")
        return f"EXTRACT(DAY FROM CAST({datetime_expr} AS TIMESTAMP))"
    
    def hour_from(self, datetime_expr: Any) -> str:
        """
        CQL 'hour from' operator - extract hour component from datetime.
        
        Example: hour from @2023-12-25T10:30:00 → 10
        """
        logger.debug("Generating CQL hour from operation")
        return f"EXTRACT(HOUR FROM CAST({datetime_expr} AS TIMESTAMP))"
    
    def minute_from(self, datetime_expr: Any) -> str:
        """
        CQL 'minute from' operator - extract minute component from datetime.
        
        Example: minute from @2023-12-25T10:30:00 → 30
        """
        logger.debug("Generating CQL minute from operation")
        return f"EXTRACT(MINUTE FROM CAST({datetime_expr} AS TIMESTAMP))"
    
    def second_from(self, datetime_expr: Any) -> str:
        """
        CQL 'second from' operator - extract second component from datetime.
        
        Example: second from @2023-12-25T10:30:15 → 15
        """
        logger.debug("Generating CQL second from operation")
        return f"EXTRACT(SECOND FROM CAST({datetime_expr} AS TIMESTAMP))"
    
    def date_from(self, datetime_expr: Any) -> str:
        """
        CQL 'date from' operator - extract date part from datetime.
        
        Example: date from @2023-12-25T10:30:00 → @2023-12-25
        """
        logger.debug("Generating CQL date from operation")
        return f"CAST({datetime_expr} AS DATE)"
    
    def time_from(self, datetime_expr: Any) -> str:
        """
        CQL 'time from' operator - extract time part from datetime.
        
        Example: time from @2023-12-25T10:30:00 → @T10:30:00
        """
        logger.debug("Generating CQL time from operation")
        return f"CAST({datetime_expr} AS TIME)"
    
    # Duration Calculation Functions
    
    def years_between(self, start_expr: Any, end_expr: Any) -> str:
        """
        CQL 'years between' function - calculate number of years between dates.
        
        Example: years between @2020-01-01 and @2023-01-01 → 3
        """
        logger.debug("Generating CQL years between operation")
        
        if self.dialect == "postgresql":
            return f"""
            EXTRACT(YEAR FROM AGE(CAST({end_expr} AS TIMESTAMP), CAST({start_expr} AS TIMESTAMP)))
            """.strip()
        else:  # DuckDB
            return f"""
            DATE_DIFF('year', CAST({start_expr} AS TIMESTAMP), CAST({end_expr} AS TIMESTAMP))
            """.strip()
    
    def months_between(self, start_expr: Any, end_expr: Any) -> str:
        """
        CQL 'months between' function - calculate number of months between dates.
        
        Example: months between @2023-01-01 and @2023-12-01 → 11
        """
        logger.debug("Generating CQL months between operation")
        
        if self.dialect == "postgresql":
            return f"""
            (EXTRACT(YEAR FROM AGE(CAST({end_expr} AS TIMESTAMP), CAST({start_expr} AS TIMESTAMP))) * 12 +
             EXTRACT(MONTH FROM AGE(CAST({end_expr} AS TIMESTAMP), CAST({start_expr} AS TIMESTAMP))))
            """.strip()
        else:  # DuckDB
            return f"""
            DATE_DIFF('month', CAST({start_expr} AS TIMESTAMP), CAST({end_expr} AS TIMESTAMP))
            """.strip()
    
    def days_between(self, start_expr: Any, end_expr: Any) -> str:
        """
        CQL 'days between' function - calculate number of days between dates.
        
        Example: days between @2023-01-01 and @2023-01-10 → 9
        """
        logger.debug("Generating CQL days between operation")
        
        if self.dialect == "postgresql":
            return f"""
            EXTRACT(DAY FROM (CAST({end_expr} AS TIMESTAMP) - CAST({start_expr} AS TIMESTAMP)))
            """.strip()
        else:  # DuckDB
            return f"""
            DATE_DIFF('day', CAST({start_expr} AS TIMESTAMP), CAST({end_expr} AS TIMESTAMP))
            """.strip()
    
    def hours_between(self, start_expr: Any, end_expr: Any) -> str:
        """
        CQL 'hours between' function - calculate number of hours between datetimes.
        
        Example: hours between @2023-01-01T10:00:00 and @2023-01-01T15:00:00 → 5
        """
        logger.debug("Generating CQL hours between operation")
        
        if self.dialect == "postgresql":
            return f"""
            EXTRACT(EPOCH FROM (CAST({end_expr} AS TIMESTAMP) - CAST({start_expr} AS TIMESTAMP))) / 3600
            """.strip()
        else:  # DuckDB
            return f"""
            DATE_DIFF('hour', CAST({start_expr} AS TIMESTAMP), CAST({end_expr} AS TIMESTAMP))
            """.strip()
    
    def minutes_between(self, start_expr: Any, end_expr: Any) -> str:
        """
        CQL 'minutes between' function - calculate number of minutes between datetimes.
        """
        logger.debug("Generating CQL minutes between operation")
        
        if self.dialect == "postgresql":
            return f"""
            EXTRACT(EPOCH FROM (CAST({end_expr} AS TIMESTAMP) - CAST({start_expr} AS TIMESTAMP))) / 60
            """.strip()
        else:  # DuckDB
            return f"""
            DATE_DIFF('minute', CAST({start_expr} AS TIMESTAMP), CAST({end_expr} AS TIMESTAMP))
            """.strip()
    
    def seconds_between(self, start_expr: Any, end_expr: Any) -> str:
        """
        CQL 'seconds between' function - calculate number of seconds between datetimes.
        """
        logger.debug("Generating CQL seconds between operation")
        
        if self.dialect == "postgresql":
            return f"""
            EXTRACT(EPOCH FROM (CAST({end_expr} AS TIMESTAMP) - CAST({start_expr} AS TIMESTAMP)))
            """.strip()
        else:  # DuckDB
            return f"""
            DATE_DIFF('second', CAST({start_expr} AS TIMESTAMP), CAST({end_expr} AS TIMESTAMP))
            """.strip()
    
    # Difference Calculation Functions (Boundary Crossings)
    
    def difference_in_years(self, start_expr: Any, end_expr: Any) -> str:
        """
        CQL 'difference in years between' function - count year boundaries crossed.
        
        Example: difference in years between @2020-06-15 and @2023-01-10 → 2
        (2020→2021 and 2021→2022 boundaries crossed, not 2022→2023)
        """
        logger.debug("Generating CQL difference in years operation")
        
        # Calculate the difference in year components
        return f"""
        (EXTRACT(YEAR FROM CAST({end_expr} AS TIMESTAMP)) - 
         EXTRACT(YEAR FROM CAST({start_expr} AS TIMESTAMP)))
        """.strip()
    
    def difference_in_months(self, start_expr: Any, end_expr: Any) -> str:
        """
        CQL 'difference in months between' function - count month boundaries crossed.
        
        Example: difference in months between @2023-01-15 and @2023-03-10 → 2
        (Jan→Feb and Feb→Mar boundaries crossed)
        """
        logger.debug("Generating CQL difference in months operation")
        
        return f"""
        ((EXTRACT(YEAR FROM CAST({end_expr} AS TIMESTAMP)) - 
          EXTRACT(YEAR FROM CAST({start_expr} AS TIMESTAMP))) * 12 +
         (EXTRACT(MONTH FROM CAST({end_expr} AS TIMESTAMP)) - 
          EXTRACT(MONTH FROM CAST({start_expr} AS TIMESTAMP))))
        """.strip()
    
    def difference_in_days(self, start_expr: Any, end_expr: Any) -> str:
        """
        CQL 'difference in days between' function - count day boundaries crossed.
        """
        logger.debug("Generating CQL difference in days operation")
        
        # Use the same logic as days_between for day boundaries
        return self.days_between(start_expr, end_expr)
    
    def difference_in_hours(self, start_expr: Any, end_expr: Any) -> str:
        """
        CQL 'difference in hours between' function - count hour boundaries crossed.
        """
        logger.debug("Generating CQL difference in hours operation")
        
        # Use truncated hour difference for boundary counting
        return f"""
        FLOOR({self.hours_between(start_expr, end_expr)})
        """.strip()
    
    def difference_in_minutes(self, start_expr: Any, end_expr: Any) -> str:
        """
        CQL 'difference in minutes between' function - count minute boundaries crossed.
        """
        logger.debug("Generating CQL difference in minutes operation")
        
        return f"""
        FLOOR({self.minutes_between(start_expr, end_expr)})
        """.strip()
    
    def difference_in_seconds(self, start_expr: Any, end_expr: Any) -> str:
        """
        CQL 'difference in seconds between' function - count second boundaries crossed.
        """
        logger.debug("Generating CQL difference in seconds operation")
        
        return f"""
        FLOOR({self.seconds_between(start_expr, end_expr)})
        """.strip()
    
    # Temporal Constructors
    
    def datetime_constructor(self, *args: Any) -> str:
        """
        CQL DateTime constructor - create DateTime from components.
        
        Supports:
        - DateTime(year) → @year
        - DateTime(year, month) → @year-month
        - DateTime(year, month, day) → @year-month-day
        - DateTime(year, month, day, hour, minute) → @year-month-dayThour:minute
        - DateTime(year, month, day, hour, minute, second) → @year-month-dayThour:minute:second
        """
        logger.debug(f"Generating CQL DateTime constructor with {len(args)} arguments")
        
        if len(args) == 1:
            # DateTime(year)
            return f"CAST(CONCAT({args[0]}, '-01-01') AS TIMESTAMP)"
        elif len(args) == 2:
            # DateTime(year, month)
            return f"CAST(CONCAT({args[0]}, '-', LPAD(CAST({args[1]} AS VARCHAR), 2, '0'), '-01') AS TIMESTAMP)"
        elif len(args) == 3:
            # DateTime(year, month, day)
            return f"""CAST(CONCAT({args[0]}, '-', 
                      LPAD(CAST({args[1]} AS VARCHAR), 2, '0'), '-',
                      LPAD(CAST({args[2]} AS VARCHAR), 2, '0')) AS TIMESTAMP)"""
        elif len(args) == 5:
            # DateTime(year, month, day, hour, minute)
            return f"""CAST(CONCAT({args[0]}, '-', 
                      LPAD(CAST({args[1]} AS VARCHAR), 2, '0'), '-',
                      LPAD(CAST({args[2]} AS VARCHAR), 2, '0'), 'T',
                      LPAD(CAST({args[3]} AS VARCHAR), 2, '0'), ':',
                      LPAD(CAST({args[4]} AS VARCHAR), 2, '0'), ':00') AS TIMESTAMP)"""
        elif len(args) == 6:
            # DateTime(year, month, day, hour, minute, second)
            return f"""CAST(CONCAT({args[0]}, '-', 
                      LPAD(CAST({args[1]} AS VARCHAR), 2, '0'), '-',
                      LPAD(CAST({args[2]} AS VARCHAR), 2, '0'), 'T',
                      LPAD(CAST({args[3]} AS VARCHAR), 2, '0'), ':',
                      LPAD(CAST({args[4]} AS VARCHAR), 2, '0'), ':',
                      LPAD(CAST({args[5]} AS VARCHAR), 2, '0')) AS TIMESTAMP)"""
        else:
            raise ValueError(f"DateTime constructor supports 1-6 arguments, got {len(args)}")
    
    def date_constructor(self, *args: Any) -> str:
        """
        CQL Date constructor - create Date from components.
        
        Supports:
        - Date(year, month, day) → @year-month-day
        """
        logger.debug(f"Generating CQL Date constructor with {len(args)} arguments")
        
        if len(args) == 3:
            return f"""CAST(CONCAT({args[0]}, '-', 
                      LPAD(CAST({args[1]} AS VARCHAR), 2, '0'), '-',
                      LPAD(CAST({args[2]} AS VARCHAR), 2, '0')) AS DATE)"""
        else:
            raise ValueError(f"Date constructor requires exactly 3 arguments, got {len(args)}")
    
    def time_constructor(self, *args: Any) -> str:
        """
        CQL Time constructor - create Time from components.
        
        Supports:
        - Time(hour, minute) → @Thour:minute
        - Time(hour, minute, second) → @Thour:minute:second
        """
        logger.debug(f"Generating CQL Time constructor with {len(args)} arguments")
        
        if len(args) == 2:
            return f"""CAST(CONCAT(LPAD(CAST({args[0]} AS VARCHAR), 2, '0'), ':',
                      LPAD(CAST({args[1]} AS VARCHAR), 2, '0'), ':00') AS TIME)"""
        elif len(args) == 3:
            return f"""CAST(CONCAT(LPAD(CAST({args[0]} AS VARCHAR), 2, '0'), ':',
                      LPAD(CAST({args[1]} AS VARCHAR), 2, '0'), ':',
                      LPAD(CAST({args[2]} AS VARCHAR), 2, '0')) AS TIME)"""
        else:
            raise ValueError(f"Time constructor requires 2-3 arguments, got {len(args)}")
    
    # Current Date/Time Functions (Enhanced)
    
    def now(self) -> str:
        """CQL Now() function - current timestamp."""
        logger.debug("Generating CQL Now() function")
        return "CURRENT_TIMESTAMP"
    
    def today(self) -> str:
        """CQL Today() function - current date."""
        logger.debug("Generating CQL Today() function")
        return "CURRENT_DATE"
    
    def time_of_day(self) -> str:
        """CQL TimeOfDay() function - current time."""
        logger.debug("Generating CQL TimeOfDay() function")
        return "CURRENT_TIME"
    
    # Temporal Arithmetic Functions
    
    def add_years(self, datetime_expr: Any, years: Any) -> str:
        """Add years to a date/datetime."""
        logger.debug("Generating CQL add years operation")
        
        if self.dialect == "postgresql":
            return f"(CAST({datetime_expr} AS TIMESTAMP) + INTERVAL '{years} years')"
        else:  # DuckDB
            return f"(CAST({datetime_expr} AS TIMESTAMP) + INTERVAL ({years}) YEAR)"
    
    def add_months(self, datetime_expr: Any, months: Any) -> str:
        """Add months to a date/datetime."""
        logger.debug("Generating CQL add months operation")
        
        if self.dialect == "postgresql":
            return f"(CAST({datetime_expr} AS TIMESTAMP) + INTERVAL '{months} months')"
        else:  # DuckDB
            return f"(CAST({datetime_expr} AS TIMESTAMP) + INTERVAL ({months}) MONTH)"
    
    def add_days(self, datetime_expr: Any, days: Any) -> str:
        """Add days to a date/datetime."""
        logger.debug("Generating CQL add days operation")
        
        if self.dialect == "postgresql":
            return f"(CAST({datetime_expr} AS TIMESTAMP) + INTERVAL '{days} days')"
        else:  # DuckDB
            return f"(CAST({datetime_expr} AS TIMESTAMP) + INTERVAL ({days}) DAY)"
    
    def add_hours(self, datetime_expr: Any, hours: Any) -> str:
        """Add hours to a datetime."""
        logger.debug("Generating CQL add hours operation")
        
        if self.dialect == "postgresql":
            return f"(CAST({datetime_expr} AS TIMESTAMP) + INTERVAL '{hours} hours')"
        else:  # DuckDB
            return f"(CAST({datetime_expr} AS TIMESTAMP) + INTERVAL ({hours}) HOUR)"
    
    def add_minutes(self, datetime_expr: Any, minutes: Any) -> str:
        """Add minutes to a datetime."""
        logger.debug("Generating CQL add minutes operation")
        
        if self.dialect == "postgresql":
            return f"(CAST({datetime_expr} AS TIMESTAMP) + INTERVAL '{minutes} minutes')"
        else:  # DuckDB
            return f"(CAST({datetime_expr} AS TIMESTAMP) + INTERVAL ({minutes}) MINUTE)"
    
    def add_seconds(self, datetime_expr: Any, seconds: Any) -> str:
        """Add seconds to a datetime."""
        logger.debug("Generating CQL add seconds operation")
        
        if self.dialect == "postgresql":
            return f"(CAST({datetime_expr} AS TIMESTAMP) + INTERVAL '{seconds} seconds')"
        else:  # DuckDB
            return f"(CAST({datetime_expr} AS TIMESTAMP) + INTERVAL ({seconds}) SECOND)"
    
    # Temporal Boundary Functions
    
    def start_of_year(self, datetime_expr: Any) -> str:
        """Get start of year for given date/datetime."""
        logger.debug("Generating CQL start of year operation")
        
        return f"""
        CAST(CONCAT(EXTRACT(YEAR FROM CAST({datetime_expr} AS TIMESTAMP)), '-01-01') AS TIMESTAMP)
        """.strip()
    
    def end_of_year(self, datetime_expr: Any) -> str:
        """Get end of year for given date/datetime."""
        logger.debug("Generating CQL end of year operation")
        
        return f"""
        CAST(CONCAT(EXTRACT(YEAR FROM CAST({datetime_expr} AS TIMESTAMP)), '-12-31T23:59:59') AS TIMESTAMP)
        """.strip()
    
    def start_of_month(self, datetime_expr: Any) -> str:
        """Get start of month for given date/datetime."""
        logger.debug("Generating CQL start of month operation")
        
        return f"""
        CAST(CONCAT(EXTRACT(YEAR FROM CAST({datetime_expr} AS TIMESTAMP)), '-',
                   LPAD(CAST(EXTRACT(MONTH FROM CAST({datetime_expr} AS TIMESTAMP)) AS VARCHAR), 2, '0'),
                   '-01') AS TIMESTAMP)
        """.strip()
    
    def end_of_month(self, datetime_expr: Any) -> str:
        """Get end of month for given date/datetime."""
        logger.debug("Generating CQL end of month operation")
        
        if self.dialect == "postgresql":
            return f"""
            (DATE_TRUNC('month', CAST({datetime_expr} AS TIMESTAMP)) + 
             INTERVAL '1 month' - INTERVAL '1 second')
            """.strip()
        else:  # DuckDB
            return f"""
            (DATE_TRUNC('month', CAST({datetime_expr} AS TIMESTAMP)) + 
             INTERVAL 1 MONTH - INTERVAL 1 SECOND)
            """.strip()
    
    def start_of_day(self, datetime_expr: Any) -> str:
        """Get start of day (midnight) for given date/datetime."""
        logger.debug("Generating CQL start of day operation")
        
        return f"DATE_TRUNC('day', CAST({datetime_expr} AS TIMESTAMP))"
    
    def end_of_day(self, datetime_expr: Any) -> str:
        """Get end of day (23:59:59) for given date/datetime."""
        logger.debug("Generating CQL end of day operation")
        
        if self.dialect == "postgresql":
            return f"""
            (DATE_TRUNC('day', CAST({datetime_expr} AS TIMESTAMP)) + 
             INTERVAL '1 day' - INTERVAL '1 second')
            """.strip()
        else:  # DuckDB
            return f"""
            (DATE_TRUNC('day', CAST({datetime_expr} AS TIMESTAMP)) + 
             INTERVAL 1 DAY - INTERVAL 1 SECOND)
            """.strip()
    
    # Utility Functions
    
    def get_supported_functions(self) -> List[str]:
        """Get list of all supported CQL date/time functions."""
        return list(self.function_map.keys())
    
    def is_temporal_function(self, function_name: str) -> bool:
        """Check if function is a temporal/date-time function."""
        temporal_functions = {
            'year', 'month', 'day', 'hour', 'minute', 'second', 'date', 'time',
            'years_between', 'months_between', 'days_between', 'hours_between', 'minutes_between', 'seconds_between',
            'difference_in_years', 'difference_in_months', 'difference_in_days', 
            'difference_in_hours', 'difference_in_minutes', 'difference_in_seconds',
            'datetime', 'date', 'time', 'now', 'today', 'timeofday',
            'add_years', 'add_months', 'add_days', 'add_hours', 'add_minutes', 'add_seconds',
            'start_of_year', 'end_of_year', 'start_of_month', 'end_of_month', 'start_of_day', 'end_of_day'
        }
        return function_name.lower() in temporal_functions
    
    def generate_cql_datetime_function_sql(self, function_name: str, args: List[Any], 
                                         dialect: str = None) -> str:
        """
        Generate SQL for CQL date/time function call.
        
        Args:
            function_name: Name of the function to call
            args: Function arguments
            dialect: Database dialect (overrides instance dialect)
            
        Returns:
            SQL expression for function call
        """
        if dialect:
            old_dialect = self.dialect
            self.dialect = dialect
        
        try:
            function_name_lower = function_name.lower()
            
            if function_name_lower in self.function_map:
                handler = self.function_map[function_name_lower]
                
                # Route to appropriate handler based on argument count
                if len(args) == 0:
                    return handler()
                elif len(args) == 1:
                    return handler(args[0])
                elif len(args) == 2:
                    return handler(args[0], args[1])
                else:
                    # Multiple arguments - constructor functions
                    return handler(*args)
            else:
                logger.warning(f"Unknown CQL date/time function: {function_name}")
                return f"-- Unknown date/time function: {function_name}({', '.join(map(str, args))})"
                
        finally:
            if dialect:
                self.dialect = old_dialect