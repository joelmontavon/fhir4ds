"""
Result Formatters Module

Provides easy export functionality for FHIR query results to various formats:
- Pandas DataFrames with FHIR-aware formatting
- JSON with proper data type handling
- CSV with configurable delimiters
- Excel with multiple sheets and formatting
- Parquet for analytics workflows
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
from datetime import datetime

# Optional imports with graceful fallbacks
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    pd = None

try:
    import openpyxl
    EXCEL_AVAILABLE = True
except ImportError:
    EXCEL_AVAILABLE = False

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    PARQUET_AVAILABLE = True
except ImportError:
    PARQUET_AVAILABLE = False

logger = logging.getLogger(__name__)


class ResultFormatter:
    """
    Handles formatting and exporting FHIR query results to various formats.
    
    Provides methods for converting FHIRResultSet objects to different output formats
    with FHIR-aware data type handling and proper metadata preservation.
    """
    
    @staticmethod
    def to_dataframe(result, include_metadata: bool = True) -> 'pd.DataFrame':
        """
        Convert query results to pandas DataFrame.
        
        Args:
            result: FHIRResultSet or similar result object
            include_metadata: Whether to include query metadata as DataFrame attributes
            
        Returns:
            pandas.DataFrame with FHIR-aware formatting
            
        Raises:
            ImportError: If pandas is not available
            
        Example:
            >>> df = ResultFormatter.to_dataframe(query_result)
            >>> print(df.head())
        """
        if not PANDAS_AVAILABLE:
            raise ImportError(
                "pandas is required for DataFrame export. Install with: pip install pandas"
            )
        
        # Use the built-in to_df method if available
        if hasattr(result, 'to_df'):
            return result.to_df(include_metadata=include_metadata)
        
        # Fallback for other result types
        if hasattr(result, 'fetchall'):
            rows = result.fetchall()
            
            # Get column names from description if available
            if hasattr(result, 'description') and result.description:
                columns = [desc[0] for desc in result.description]
            else:
                # Generate generic column names
                if rows:
                    columns = [f"col_{i}" for i in range(len(rows[0]))]
                else:
                    columns = []
            
            df = pd.DataFrame(rows, columns=columns) if rows else pd.DataFrame(columns=columns)
            
            # Add metadata if available and requested
            if include_metadata and hasattr(result, 'sql'):
                df.attrs['sql_query'] = result.sql
                df.attrs['export_timestamp'] = datetime.now().isoformat()
            
            return df
        
        raise ValueError(f"Cannot convert result type {type(result)} to DataFrame")
    
    @staticmethod
    def to_json(result, output_path: Optional[Union[str, Path]] = None, 
                include_metadata: bool = True, indent: int = 2) -> Union[str, None]:
        """
        Export query results to JSON format.
        
        Args:
            result: FHIRResultSet or similar result object
            output_path: Optional file path to save JSON. If None, returns JSON string.
            include_metadata: Whether to include query metadata in output
            indent: JSON indentation level
            
        Returns:
            JSON string if output_path is None, otherwise None
            
        Example:
            >>> json_str = ResultFormatter.to_json(query_result)
            >>> ResultFormatter.to_json(query_result, "output.json")
        """
        # Get rows and column information
        if hasattr(result, 'fetchall'):
            rows = result.fetchall()
        else:
            raise ValueError(f"Cannot extract rows from result type {type(result)}")
        
        # Get column names
        if hasattr(result, 'description') and result.description:
            columns = [desc[0] for desc in result.description]
        else:
            if rows:
                columns = [f"col_{i}" for i in range(len(rows[0]))]
            else:
                columns = []
        
        # Convert to list of dictionaries
        data = []
        for row in rows:
            row_dict = {}
            for i, value in enumerate(row):
                if i < len(columns):
                    row_dict[columns[i]] = value
            data.append(row_dict)
        
        # Create output structure
        output = {
            "data": data,
            "row_count": len(data),
            "column_count": len(columns),
            "columns": columns
        }
        
        # Add metadata if requested
        if include_metadata:
            metadata = {
                "export_timestamp": datetime.now().isoformat(),
                "format": "json"
            }
            if hasattr(result, 'sql'):
                metadata["sql_query"] = result.sql
            output["metadata"] = metadata
        
        # Convert to JSON string
        json_str = json.dumps(output, indent=indent, default=str)
        
        # Save to file if path provided
        if output_path:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w') as f:
                f.write(json_str)
            logger.info(f"JSON exported to {output_path}")
            return None
        
        return json_str
    
    @staticmethod
    def to_csv(result, output_path: Optional[Union[str, Path]] = None, 
               delimiter: str = ',', include_header: bool = True,
               include_metadata: bool = False) -> Optional[str]:
        """
        Export query results to CSV format.
        
        Args:
            result: FHIRResultSet or similar result object
            output_path: File path to save CSV (optional - returns string if None)
            delimiter: CSV delimiter character
            include_header: Whether to include column headers
            include_metadata: Whether to add metadata as comments
            
        Returns:
            If output_path is None, returns CSV as string. Otherwise saves to file and returns None.
            
        Example:
            >>> ResultFormatter.to_csv(query_result, "output.csv")
            >>> csv_string = ResultFormatter.to_csv(query_result)
        """
        import csv
        import io
        
        # Determine if we're writing to file or string
        write_to_file = output_path is not None
        
        if write_to_file:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Get data
        if hasattr(result, 'fetchall'):
            rows = result.fetchall()
        else:
            raise ValueError(f"Cannot extract rows from result type {type(result)}")
        
        # Get column names
        if hasattr(result, 'description') and result.description:
            columns = [desc[0] for desc in result.description]
        else:
            if rows:
                columns = [f"col_{i}" for i in range(len(rows[0]))]
            else:
                columns = []
        
        # Choose output destination
        if write_to_file:
            output_file = open(output_path, 'w', newline='', encoding='utf-8')
        else:
            output_file = io.StringIO()
        
        try:
            writer = csv.writer(output_file, delimiter=delimiter)
            
            # Write metadata as comments if requested
            if include_metadata:
                from datetime import datetime
                output_file.write(f"# Export timestamp: {datetime.now().isoformat()}\n")
                output_file.write(f"# Row count: {len(rows)}\n")
                output_file.write(f"# Column count: {len(columns)}\n")
                if hasattr(result, 'sql'):
                    output_file.write(f"# SQL query: {result.sql}\n")
                output_file.write("# \n")
            
            # Write header if requested
            if include_header and columns:
                writer.writerow(columns)
            
            # Write data rows
            for row in rows:
                writer.writerow(row)
            
            # Return result based on output type
            if write_to_file:
                return None
            else:
                return output_file.getvalue()
                
        finally:
            if write_to_file:
                output_file.close()
        
        logger.info(f"CSV exported to {output_path} ({len(rows)} rows, {len(columns)} columns)")
    
    @staticmethod
    def to_excel(results: Union[Any, List[Any]], output_path: Union[str, Path],
                 sheet_names: Optional[List[str]] = None, 
                 include_metadata: bool = True) -> None:
        """
        Export query results to Excel format with multiple sheets support.
        
        Args:
            results: Single result or list of results to export
            output_path: File path to save Excel file
            sheet_names: Optional list of sheet names. If None, generates default names.
            include_metadata: Whether to include metadata sheet
            
        Raises:
            ImportError: If openpyxl is not available
            
        Example:
            >>> # Single sheet
            >>> ResultFormatter.to_excel(query_result, "output.xlsx")
            
            >>> # Multiple sheets
            >>> ResultFormatter.to_excel([result1, result2], "report.xlsx", 
            ...                         sheet_names=["Patients", "Observations"])
        """
        if not EXCEL_AVAILABLE:
            raise ImportError(
                "openpyxl is required for Excel export. Install with: pip install openpyxl"
            )
        
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Ensure results is a list
        if not isinstance(results, list):
            results = [results]
        
        # Generate sheet names if not provided
        if not sheet_names:
            sheet_names = [f"Sheet_{i+1}" for i in range(len(results))]
        elif len(sheet_names) != len(results):
            raise ValueError(f"Number of sheet names ({len(sheet_names)}) must match number of results ({len(results)})")
        
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            metadata_info = []
            
            for i, result in enumerate(results):
                sheet_name = sheet_names[i]
                
                # Convert to DataFrame
                try:
                    df = ResultFormatter.to_dataframe(result, include_metadata=False)
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    
                    # Collect metadata for summary sheet
                    if include_metadata:
                        sheet_metadata = {
                            'sheet_name': sheet_name,
                            'row_count': len(df),
                            'column_count': len(df.columns),
                            'columns': list(df.columns)
                        }
                        if hasattr(result, 'sql'):
                            sheet_metadata['sql_query'] = result.sql
                        metadata_info.append(sheet_metadata)
                    
                    logger.info(f"Sheet '{sheet_name}' exported ({len(df)} rows, {len(df.columns)} columns)")
                
                except Exception as e:
                    logger.error(f"Failed to export sheet '{sheet_name}': {e}")
                    # Create empty sheet with error message
                    error_df = pd.DataFrame({'Error': [f"Failed to export: {str(e)}"]})
                    error_df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            # Add metadata sheet if requested
            if include_metadata and metadata_info:
                metadata_df = pd.DataFrame(metadata_info)
                metadata_df.to_excel(writer, sheet_name='_Metadata', index=False)
                logger.info("Metadata sheet added")
        
        logger.info(f"Excel file exported to {output_path}")
    
    @staticmethod
    def to_parquet(result, output_path: Union[str, Path], 
                   compression: str = 'snappy', include_metadata: bool = True) -> None:
        """
        Export query results to Parquet format for analytics workflows.
        
        Args:
            result: FHIRResultSet or similar result object
            output_path: File path to save Parquet file
            compression: Compression algorithm ('snappy', 'gzip', 'brotli', 'lz4')
            include_metadata: Whether to include query metadata
            
        Raises:
            ImportError: If pyarrow is not available
            
        Example:
            >>> ResultFormatter.to_parquet(query_result, "output.parquet")
            >>> ResultFormatter.to_parquet(query_result, "output.parquet", compression="gzip")
        """
        if not PARQUET_AVAILABLE:
            raise ImportError(
                "pyarrow is required for Parquet export. Install with: pip install pyarrow"
            )
        
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Convert to DataFrame first
        df = ResultFormatter.to_dataframe(result, include_metadata=False)
        
        # Create PyArrow table
        table = pa.Table.from_pandas(df)
        
        # Add metadata if requested
        if include_metadata:
            metadata = {
                'export_timestamp': datetime.now().isoformat(),
                'format': 'parquet',
                'compression': compression,
                'row_count': str(len(df)),
                'column_count': str(len(df.columns))
            }
            if hasattr(result, 'sql'):
                metadata['sql_query'] = result.sql
            
            # Add metadata to table schema
            existing_metadata = table.schema.metadata or {}
            existing_metadata.update({k.encode(): v.encode() for k, v in metadata.items()})
            table = table.replace_schema_metadata(existing_metadata)
        
        # Write to file
        pq.write_table(table, output_path, compression=compression)
        
        logger.info(f"Parquet exported to {output_path} ({len(df)} rows, {len(df.columns)} columns, {compression} compression)")


class FHIRDataTypeHandler:
    """
    Handles FHIR-specific data type formatting and validation.
    
    Provides utilities for proper handling of FHIR data types during export:
    - Date/DateTime formatting
    - Boolean normalization
    - Code system handling
    - Reference formatting
    """
    
    @staticmethod
    def normalize_fhir_boolean(value: Any) -> Optional[bool]:
        """
        Normalize various representations of FHIR boolean values.
        
        Args:
            value: Value to normalize (could be string, bool, etc.)
            
        Returns:
            Normalized boolean value or None
        """
        if value is None:
            return None
        
        if isinstance(value, bool):
            return value
        
        if isinstance(value, str):
            lower_val = value.lower()
            if lower_val in ('true', '1', 'yes', 'on'):
                return True
            elif lower_val in ('false', '0', 'no', 'off'):
                return False
        
        return None
    
    @staticmethod
    def format_fhir_datetime(value: Any, target_format: str = 'iso') -> Optional[str]:
        """
        Format FHIR date/dateTime values consistently.
        
        Args:
            value: Date/datetime value to format
            target_format: Target format ('iso', 'date_only', 'time_only')
            
        Returns:
            Formatted date string or None
        """
        if value is None:
            return None
        
        # This is a placeholder for more sophisticated date handling
        # In practice, would parse various FHIR date formats and standardize
        return str(value)
    
    @staticmethod
    def extract_code_display(coding: Any) -> Optional[str]:
        """
        Extract display text from FHIR Coding elements.
        
        Args:
            coding: FHIR Coding element (dict or JSON string)
            
        Returns:
            Display text or code if display not available
        """
        if isinstance(coding, str):
            try:
                coding = json.loads(coding)
            except:
                return coding
        
        if isinstance(coding, dict):
            return coding.get('display') or coding.get('code')
        
        return None