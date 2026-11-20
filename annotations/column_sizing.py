"""Helpers for schema_update.py."""


def reset_for_new_type(column):
    """Clear the attributes of a fetched Column that belong to its old type.

    schema_update.py changes a column's type by editing the fetched Column in
    place (keeping its identity so the table change replaces the old column).
    maximum_size, enum_values, maximum_list_length, default_value and the
    facet/JSON settings would otherwise carry over from the old type; the
    previous implementation built a fresh Column with only a name, the new
    type and, where it applies, maximum_size. Callers set those afterwards.
    """
    column.maximum_size = None
    column.maximum_list_length = None
    column.enum_values = None
    column.default_value = None
    column.facet_type = None
    column.json_sub_columns = None
    return column
