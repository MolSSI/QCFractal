import pytest


@pytest.fixture(scope="function", params=[True, False])
def spec_test_runner(storage_socket, request):

    def _run_test(record_type, spec_1, spec_2, expected_same: bool):
        socket = storage_socket.records.get_socket(record_type)
        if request.param:
            meta, ids = socket.add_specifications([spec_1, spec_2, spec_1, spec_2, spec_1, spec_1, spec_2])
            assert meta.success
            if expected_same:
                assert meta.inserted_idx == [0]
                assert meta.existing_idx == [1, 2, 3, 4, 5, 6]
                assert all(x == ids[0] for x in ids)
            else:
                assert meta.inserted_idx == [0, 1]
                assert meta.existing_idx == [2, 3, 4, 5, 6]
                assert ids[0] == ids[2]
                assert ids[0] == ids[4]
                assert ids[0] == ids[5]

                assert ids[1] == ids[3]
                assert ids[1] == ids[6]

                assert ids[0] != ids[1]

            # Try adding again
            meta, ids = socket.add_specifications([spec_1, spec_2, spec_1, spec_2, spec_1, spec_1, spec_2])
            assert meta.success
            assert meta.inserted_idx == []
            assert meta.existing_idx == [0, 1, 2, 3, 4, 5, 6]
            if expected_same:
                assert all(x == ids[0] for x in ids)
            else:
                assert ids[0] == ids[2]
                assert ids[0] == ids[4]
                assert ids[0] == ids[5]

                assert ids[1] == ids[3]
                assert ids[1] == ids[6]

                assert ids[0] != ids[1]

        else:
            meta1, id1 = socket.add_specification(spec_1)
            meta2, id2 = socket.add_specification(spec_2)

            assert meta1.success
            assert meta2.success

            assert meta1.inserted_idx == [0]
            assert meta2.inserted_idx == ([] if expected_same else [0])
            assert meta2.existing_idx == ([0] if expected_same else [])
            assert id1 == id2 if expected_same else id1 != id2

    return _run_test
