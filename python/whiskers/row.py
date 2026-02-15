class Row:
    """
    A row of data from a cursor fetch operation. Provides both tuple-like indexing
    and attribute access to column values.
    """

    __slots__ = ('_values', '_column_map', '_cursor_ref')

    def __init__(self, cursor, description, values, column_map=None):
        self._values = values
        self._column_map = column_map
        self._cursor_ref = cursor
    
    def _apply_output_converters(self, cursor, description):
        """Apply output converters to raw values in-place."""
        if not description:
            return
        
        values = self._values
        if isinstance(values, tuple):
            values = list(values)
            self._values = values
        
        for i, (value, desc) in enumerate(zip(values, description)):
            if desc is None or value is None:
                continue
            
            sql_type = desc[1]
            converter = cursor.connection.get_output_converter(sql_type)
            
            if converter is None and isinstance(value, (str, bytes)):
                from whiskers.constants import ConstantsDDBC
                converter = cursor.connection.get_output_converter(ConstantsDDBC.SQL_WVARCHAR.value)
            
            if converter:
                try:
                    if isinstance(value, str):
                        value_bytes = value.encode('utf-16-le')
                        values[i] = converter(value_bytes)
                    else:
                        values[i] = converter(value)
                except Exception:
                    pass

    def __getitem__(self, index):
        return self._values[index]
    
    def __getattr__(self, name):
        try:
            column_map = object.__getattribute__(self, '_column_map')
        except AttributeError:
            raise AttributeError(f"Row has no attribute '{name}'")
        
        if column_map is not None and name in column_map:
            return object.__getattribute__(self, '_values')[column_map[name]]
        
        cursor = object.__getattribute__(self, '_cursor_ref')
        if hasattr(cursor, 'lowercase') and cursor.lowercase:
            name_lower = name.lower()
            for col_name in column_map:
                if col_name.lower() == name_lower:
                    return object.__getattribute__(self, '_values')[column_map[col_name]]
        
        raise AttributeError(f"Row has no attribute '{name}'")
    
    @property
    def cursor_description(self):
        """For backward compatibility."""
        return self._cursor_ref.description if self._cursor_ref else None

    def __eq__(self, other):
        if isinstance(other, list):
            return list(self._values) == other
        elif isinstance(other, Row):
            return self._values == other._values
        return super().__eq__(other)
    
    def __len__(self):
        return len(self._values)
    
    def __iter__(self):
        return iter(self._values)
    
    def __str__(self):
        from decimal import Decimal
        from whiskers import getDecimalSeparator
        
        parts = []
        for value in self:
            if isinstance(value, Decimal):
                sep = getDecimalSeparator()
                if sep != '.' and value is not None:
                    s = str(value)
                    if '.' in s:
                        s = s.replace('.', sep)
                    parts.append(s)
                else:
                    parts.append(str(value))
            else:
                parts.append(repr(value))
        
        return "(" + ", ".join(parts) + ")"

    def __repr__(self):
        return repr(tuple(self._values))
