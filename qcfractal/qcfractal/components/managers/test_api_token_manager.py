"""
A compute manager can authenticate to a secure server with an API token
"""

from qcportal.managers import ManagerName


def test_manager_client_api_token(secure_snowflake):
    # Mint a compute-role token
    socket = secure_snowflake.get_storage_socket()
    compute_user_id = socket.users.get("compute_user")["id"]
    raw, _ = socket.auth.create_api_token(compute_user_id, description="a manager")

    mname = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-9999")
    mclient = secure_snowflake.manager_client(mname, api_token=raw)

    # The token client learned who it is from /me
    assert mclient.username == "compute_user"

    mclient.activate(
        manager_version="v2.0",
        programs={"qcengine": ["unknown"], "qcprog": ["unknown"]},
        compute_tags=["tag1"],
    )

    # The server persisted the manager under the authenticated user
    admin = secure_snowflake.user_client("admin_user")
    manager = admin.get_managers(mname.fullname)
    assert manager.username == "compute_user"
