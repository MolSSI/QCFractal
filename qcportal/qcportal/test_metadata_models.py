from qcportal.metadata_models import InsertMetadata


def test_insert_metadata_merge():
    m1 = InsertMetadata(inserted_idx=[0, 1], existing_idx=[2, 3, 5], errors=[(4, "error1")])
    m2 = InsertMetadata(inserted_idx=[], existing_idx=[0], errors=[])
    m3 = InsertMetadata(inserted_idx=[0], existing_idx=[], errors=[])
    m4 = InsertMetadata()
    m5 = InsertMetadata(inserted_idx=[5], existing_idx=[0, 1, 2, 3], errors=[(4, "error2")])

    all = InsertMetadata.merge([m1, m2, m3, m4, m5])

    assert all.inserted_idx == [0, 1, 7, 13]
    assert all.existing_idx == [2, 3, 5, 6, 8, 9, 10, 11]
    assert all.error_idx == [4, 12]
    assert all.errors == [(4, "error1"), (12, "error2")]
    assert all.error_description is None

    m1 = InsertMetadata(inserted_idx=[0, 1], error_description="errordesc1")
    m2 = InsertMetadata(inserted_idx=[], existing_idx=[0])
    m3 = InsertMetadata(inserted_idx=[0], error_description="errordesc2")

    all = InsertMetadata.merge([m1, m2, m3])
    assert all.error_description == "errordesc1\nerrordesc2"
