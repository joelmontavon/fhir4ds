"""
SQL Builder Classes for CTE-based Architecture

This module contains sophisticated SQL builder classes adapted from the zz implementation
to support incremental CTE building and complex forEach operations.
"""

import re
from typing import List, Optional, Union, Any


class QueryItem:
    """Base class for all SQL query components"""
    
    def __format__(self, pretty=True):
        """Format SQL with optional pretty printing"""
        try:
            import sqlparse
            return sqlparse.format(self.__str__(), reindent=True, indent_width=2) if pretty else self.__str__()
        except ImportError:
            return self.__str__()


class Literal(QueryItem):
    """SQL literal value (quoted string)"""

    def __init__(self, string: str):
        self.string = string

    def __str__(self):
        return f"'{self.string}'"

    @staticmethod
    def is_literal(string: str) -> bool:
        """Check if string is a literal (quoted)"""
        if len(string) > 0:
            return string[0] == "'" and string[-1] == "'"
        else:
            return False


class Field(QueryItem):
    """SQL field reference with optional table qualifier"""

    def __init__(self, name: str, table: Optional[Union[str, int]] = None):
        self.name = name
        self.table = f't{table}' if isinstance(table, int) else table

    def __str__(self):
        wrapper = '' if self.name == '*' else '"'
        return f'{(self.table + ".") if self.table else ""}{wrapper}{self.name}{wrapper}'

    @staticmethod
    def is_field(string: str) -> bool:
        """Check if string represents a field (not keyword or literal)"""
        keywords = ['and', 'or', 'not', '=', '>', '<', '>=', '+', '-']
        return not (string in keywords or string.isnumeric() or 
                   string.lower() in ['true', 'false'] or Literal.is_literal(string))


class Func(QueryItem):
    """SQL function call"""

    def __init__(self, name: str, args: Optional[List[Any]] = None):
        if args is None:
            args = []
        self.args = args if isinstance(args, list) else [args]
        self.name = name

    def __str__(self):
        return f'{self.name}({", ".join([str(arg) for arg in self.args])})'


class Expr(QueryItem):
    """SQL expression with custom separator"""

    def __init__(self, args: Optional[Union[List[Any], Any]] = None, sep: str = ' '):
        if args is None:
            args = []
        self.args = args if isinstance(args, (list, tuple)) else [args]
        self.sep = sep

    def __str__(self):
        return self.sep.join([(arg if isinstance(arg, str) else str(arg)) for arg in self.args])


class SelectItem(QueryItem):
    """SQL SELECT item with optional alias"""

    def __init__(self, item: QueryItem, alias: Optional[str] = None):
        self.item = item
        self.alias = self._get_alias(item, alias)
        
    def _get_alias(self, item: QueryItem, alias: Optional[str], default: str = 'f') -> str:
        """Generate appropriate alias for select item"""
        if isinstance(alias, Select):
            alias.j += 1
            return default + str(alias.j)
        elif alias:
            return alias
        elif isinstance(item, Field):
            return item.name
        elif isinstance(item, (Func, Expr)) and len(item.args) > 0 and isinstance(item.args[0], Field):
            return item.args[0].name
        else:
            return default

    def __str__(self):
        if self.alias == '*':
            return str(self.item)
        elif isinstance(self.item, Field) and self.item.name == self.alias:
            return str(self.item)
        elif not self.alias:
            return str(self.item)
        else:
            return f'{self.item} AS "{self.alias}"'


class Table(QueryItem):
    """SQL table reference"""

    def __init__(self, name: Union[str, int]):
        self.name = f't{name}' if isinstance(name, int) else name

    def __str__(self):
        return self.name


class Join(QueryItem):
    """SQL JOIN clause with type and lateral support"""
    
    types = {
        "INNER": "INNER JOIN",
        "OUTER": "LEFT OUTER JOIN", 
        "CROSS": "CROSS JOIN",
        None: ""
    }
        
    def __init__(self, join_type: Optional[str] = None, is_lateral: bool = True):
        self.type = join_type
        self.is_lateral = (join_type and is_lateral)
        
    def __str__(self):
        return Join.types[self.type] + (' LATERAL' if self.is_lateral else '')


class Select(QueryItem):
    """SQL SELECT statement with full clause support"""

    def __init__(self, select_items: Optional[List[SelectItem]] = None, 
                 from_items: Optional[List['FromItem']] = None,
                 where_items: Optional[List[str]] = None, 
                 cte_items: Optional[List['Cte']] = None,
                 orderby_items: Optional[List[str]] = None, 
                 limit: Optional[int] = None, 
                 offset: Optional[int] = None, 
                 distinct: Optional[bool] = None, 
                 groupby_items: Optional[List[str]] = None):
        
        self.cte_items = self._to_list(cte_items)
        self.select_items = self._to_list(select_items)
        self.from_items = self._to_list(from_items)
        self.where_items = self._to_list(where_items)
        self.groupby_items = self._to_list(groupby_items) 
        self.orderby_items = self._to_list(orderby_items) 
        self.limit = limit
        self.offset = offset
        self.distinct = distinct
        self.i = 0  # Counter for auto-generated aliases
        self.j = 0  # Counter for auto-generated field aliases

    @staticmethod
    def _to_list(x: Any) -> List:
        """Convert input to list format"""
        if x is None:
            return []
        elif not isinstance(x, list):
            return [x]
        else:
            return x
    
    def join(self, from_item: 'FromItem', to_item: 'FromItem', join: Join = Join('CROSS')):
        """Add a JOIN between FROM items"""
        if join.type == 'CROSS':
            to_item.on_items = []    
        elif join.is_lateral:
            to_item.on_items = ['True']
        else:
            # For INNER/OUTER joins, we'll set on_items manually or use a common field like 'id'
            # This is a simplified implementation - in real usage, join conditions would be specified
            to_item.on_items = []

        to_item.join = join
        self.from_items.append(to_item)

    def __str__(self):
        select_items = [Expr('*')] if len(self.select_items) == 0 else self.select_items
        
        sql = f"""{'WITH ' if len(self.cte_items) > 0 else ''}{', '.join([str(c) for c in self.cte_items]) if len(self.cte_items) > 0 else ''}
                {' ' if len(self.cte_items) > 0 else ''}SELECT {'DISTINCT ' if self.distinct else ''}{', '.join([str(s) for s in select_items])}
                {(' FROM ' + ' '.join([str(f) for f in self.from_items])) if len(self.from_items) > 0 else ''}
                {' WHERE ' if len(self.where_items) > 0 else ''}{' AND '.join([str(w) for w in self.where_items])}
                {' GROUP BY ' if len(self.groupby_items) > 0 else ''}{','.join([str(g) for g in self.groupby_items])}
                {' ORDER BY ' if len(self.orderby_items) > 0 else ''}{','.join([str(o) for o in self.orderby_items])}
                {(" LIMIT " + str(self.limit)) if self.limit else ""}
                {(" OFFSET " + str(self.offset)) if self.offset else ""}"""
        
        return re.sub(r'[ \t]+', ' ', sql)


class FromItem(QueryItem):
    """SQL FROM item (table, subquery, or CTE reference)"""

    def __init__(self, item: Union[Select, 'Union', Table], alias: Optional[str] = None):  
        
        if isinstance(item, (Select, Union)):
            self.item = item
            self.alias = self._get_alias(alias)
            self.type = type(item)
        elif isinstance(item, Table):
            self.item = Select([SelectItem(Field('*'))], item)
            self.alias = item.name
            self.type = type(item)
        else:
            raise TypeError(f'FromItem can only be created from Select, Union, or Table, not {type(item)}')

        self.join = Join()
        self.on_items = []
            
    def _get_alias(self, alias: Optional[str] = None, default: str = 't') -> str:
        """Generate appropriate alias"""
        if isinstance(alias, int):
            return f't{alias}'
        elif isinstance(alias, str):
            return alias
        elif isinstance(alias, Select):
            alias.i += 1
            return f't{alias.i}'
        else:
            return default

    def to_ref(self) -> 'TableRef':
        """Convert to table reference"""
        select_items = [SelectItem(Field(select_item.alias, self.alias)) for select_item in self.item.select_items]
        return TableRef(Select(select_items), self.alias)
    
    def to_select(self) -> Select:
        """Convert to SELECT statement"""
        select_items = [SelectItem(Field(select_item.alias, self.alias)) for select_item in self.item.select_items]
        return Select(select_items, TableRef(Select(select_items), self.alias))    

    def __str__(self):
        if self.type is Table:
            return f'{self.join} {self.alias}{" ON " if len(self.on_items) > 0 else ""}{" AND ".join(self.on_items)}'
        else:
            return f'{self.join} ({self.item}) {self.alias}{" ON " if len(self.on_items) > 0 else ""}{" AND ".join(self.on_items)}'


class TableRef(FromItem):
    """Reference to an existing table or CTE"""
    
    def __str__(self):
        return super().__str__()


class Subquery(FromItem):
    """SQL subquery"""

    def __str__(self):
        if self.type is Table:
            return super().__str__()
        else:
            return f'{self.join} ({self.item}) {self.alias}{" ON " if len(self.on_items) > 0 else ""}{" AND ".join(self.on_items)}'

    def to_cte(self) -> 'Cte':
        """Convert to CTE"""
        return Cte(self.item, self.alias)


class Cte(FromItem):
    """Common Table Expression (CTE)"""

    def __str__(self):
        return f'{self.alias} AS ({self.item})'

    def to_subquery(self) -> Subquery:
        """Convert to subquery"""
        return Subquery(self.item, self.alias)


class Union(QueryItem):
    """SQL UNION ALL operation"""

    def __init__(self, selects: Optional[List[Select]] = None):
        if selects is None:
            selects = []

        self.selects = selects
        self.select_items = [SelectItem(Field(alias, 't')) for alias in Union._get_aliases(selects)]
        self.from_items = ['t']

    @staticmethod
    def _get_aliases(selects: List[Select]) -> set:
        """Get common aliases across all SELECT statements"""
        aliases = [[select_item.alias for select_item in select.select_items] for select in selects]
        return set.intersection(*map(set, aliases)) if aliases else set()

    @staticmethod
    def _get_from_items(selects: List[Select]) -> List[Select]:
        """Get FROM items for union"""
        aliases = Union._get_aliases(selects)
        from_items = [Select([SelectItem(Field(alias, select.from_items[0].alias)) for alias in aliases], select.from_items[0]) for select in selects]
        return from_items

    def __str__(self):
        from_items = self.selects
        return f"""
            SELECT 
            {', '.join([str(s) for s in self.select_items])}
            FROM (
            {' UNION ALL '.join([f"({str(s)})" for s in from_items])}
            ) t
        """