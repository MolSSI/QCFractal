from __future__ import annotations, annotations


def test_serverinfo_motd(snowflake):
    motd_msg = "This is the message of the day" * 10
    client1 = snowflake.client()
    client1.set_motd(motd_msg)

    msg = client1.get_motd()
    assert msg == motd_msg

    client2 = snowflake.client()
    assert client2.server_info["motd"] == motd_msg
