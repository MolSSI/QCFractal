import requests

from qcarchivetesting.testing_classes import QCATestingSnowflake


def test_homepage_redirect(postgres_server, pytestconfig):
    pg_harness = postgres_server.get_new_harness("test_homepage_redirect")
    encoding = pytestconfig.getoption("--client-encoding")

    extra_config = {"homepage_redirect_url": "https://example.com"}
    snowflake = QCATestingSnowflake(pg_harness, encoding, extra_config=extra_config)

    # The URI doesn't contain any path or anything
    r = requests.get(snowflake.get_uri(), allow_redirects=False)
    assert r.status_code == 302
    assert r.headers["Location"] == "https://example.com"


def test_homepage_serve_dir(postgres_server, tmp_path, pytestconfig):
    encoding = pytestconfig.getoption("--client-encoding")

    homepage_dir = tmp_path / "homepage"
    homepage_dir.mkdir()

    index_file = str(homepage_dir / "index.html")
    other_file = str(homepage_dir / "other_file.html")

    with open(index_file, "w") as f:
        f.write("HOMEPAGE")
    with open(other_file, "w") as f:
        f.write("OTHER FILE")

    pg_harness = postgres_server.get_new_harness("test_homepage_serve_dir")
    extra_config = {"homepage_directory": str(homepage_dir)}
    snowflake = QCATestingSnowflake(pg_harness, encoding, extra_config=extra_config)

    # The URI doesn't contain any path or anything
    r = requests.get(snowflake.get_uri(), headers={"Accept": "text/html"})
    assert r.status_code == 200
    assert r.text == "HOMEPAGE"

    r = requests.get(snowflake.get_uri() + "/other_file.html", headers={"Accept": "text/html"})
    assert r.status_code == 200
    assert r.text == "OTHER FILE"

    r = requests.get(snowflake.get_uri() + "/missing.html", headers={"Accept": "text/html"})
    assert r.status_code == 404
