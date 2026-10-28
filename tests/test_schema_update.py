from synapseclient.models import Column, ColumnType

import column_sizing


def test_reset_drops_attributes_of_the_old_type():
    col = Column(
        id="1",
        name="Dataset Name",
        column_type=ColumnType.STRING,
        maximum_size=50,
        maximum_list_length=10,
        enum_values=["a", "b"],
        default_value="a",
    )
    column_sizing.reset_for_new_type(col)
    col.column_type = "LARGETEXT"
    assert col.name == "Dataset Name"
    assert col.id == "1"  # same column, so the change replaces the old one
    assert col.maximum_size is None
    assert col.maximum_list_length is None
    assert col.enum_values is None
    assert col.default_value is None
    assert col.facet_type is None
