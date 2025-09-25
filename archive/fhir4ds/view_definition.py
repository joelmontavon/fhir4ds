"""
ViewDefinition Data Models

This module contains the data classes that represent FHIR ViewDefinition
resources and their components.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Any


def _parse_column(column_data: Dict[str, Any]) -> 'Column':
    """Parse a column dictionary into a Column object."""
    return Column(
        name=column_data.get('name', ''),
        path=column_data.get('path', ''),
        type=column_data.get('type'),
        collection=column_data.get('collection', False),
        tags=column_data.get('tags')
    )


def _parse_select_structure(select_data: Dict[str, Any]) -> 'SelectStructure':
    """Parse a select structure dictionary into a SelectStructure object."""
    # Parse columns
    columns = None
    if 'column' in select_data:
        columns = [_parse_column(col) for col in select_data['column']]
    
    # Parse nested select structures
    select_structures = None
    if 'select' in select_data:
        select_structures = [_parse_select_structure(s) for s in select_data['select']]
    
    # Parse union_all structures
    union_all_structures = None
    if 'unionAll' in select_data:
        union_all_structures = [_parse_select_structure(u) for u in select_data['unionAll']]
    
    return SelectStructure(
        column=columns,
        select=select_structures,
        union_all=union_all_structures,
        for_each=select_data.get('forEach'),
        for_each_or_null=select_data.get('forEachOrNull')
    )


@dataclass
class Column:
    """Represents a column in a ViewDefinition."""
    name: str
    path: str
    type: Optional[str] = None
    collection: bool = False
    tags: Optional[List[Dict[str, str]]] = None


@dataclass
class SelectStructure:
    """Represents a select structure in a ViewDefinition."""
    column: Optional[List[Column]] = None
    select: Optional[List['SelectStructure']] = None
    union_all: Optional[List['SelectStructure']] = None
    for_each: Optional[str] = None
    for_each_or_null: Optional[str] = None


@dataclass
class ViewDefinition:
    """Represents a complete ViewDefinition."""
    name: str
    resource: str
    select: List[SelectStructure]
    where: Optional[List[Dict[str, str]]] = None
    constants: Optional[List[Dict[str, Any]]] = None
    description: Optional[str] = None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ViewDefinition':
        """Create a ViewDefinition instance from a dictionary."""
        # Parse select structures
        select_structures = []
        if 'select' in data:
            for select_data in data['select']:
                select_structures.append(_parse_select_structure(select_data))
        
        return cls(
            name=data.get('name', ''),
            resource=data.get('resource', ''),
            select=select_structures,
            where=data.get('where'),
            constants=data.get('constants'),
            description=data.get('description')
        )