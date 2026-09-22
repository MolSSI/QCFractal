import requests

from qcarchivetesting.testing_classes import QCATestingSnowflake


def test_homepage_redirect(postgres_server, client_encoding):
    pg_harness = postgres_server.get_new_harness("test_homepage_redirect")

    extra_config = {"homepage_redirect_url": "https://example.com"}
    snowflake = QCATestingSnowflake(pg_harness, client_encoding, extra_config=extra_config)

    # The URI doesn't contain any path or anything
    r = requests.get(snowflake.get_uri(), allow_redirects=False)
    assert r.status_code == 302
    assert r.headers["Location"] == "https://example.com"


def test_homepage_serve_dir(postgres_server, tmp_path, client_encoding):

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
    snowflake = QCATestingSnowflake(pg_harness, client_encoding, extra_config=extra_config)

    # The URI doesn't contain any path or anything
    r = requests.get(snowflake.get_uri(), headers={"Accept": "text/html"})
    assert r.status_code == 200
    assert r.text == "HOMEPAGE"

    r = requests.get(snowflake.get_uri() + "/other_file.html", headers={"Accept": "text/html"})
    assert r.status_code == 200
    assert r.text == "OTHER FILE"

    r = requests.get(snowflake.get_uri() + "/missing.html", headers={"Accept": "text/html"})
    assert r.status_code == 404


def test_homepage_unknown_route_404(postgres_server, client_encoding):
    # Unknown routes must return a clean 404, not a 405. The homepage blueprint's catch-all route
    # (GET only) matches every path, so Werkzeug's default routing would otherwise consider any
    # unknown, non-GET request a "match" for that route with the wrong method, and report 405
    # Method Not Allowed instead of 404.
    pg_harness = postgres_server.get_new_harness("test_homepage_unknown_route_404")
    snowflake = QCATestingSnowflake(pg_harness, client_encoding)

    for method in ("get", "post", "put", "patch", "delete"):
        r = getattr(requests, method)(snowflake.get_uri() + "/api/v1/zzz/nonexistent")
        assert r.status_code == 404, f"{method} unknown route returned {r.status_code}"

    # A known route with the wrong method should still correctly report 405
    r = requests.patch(snowflake.get_uri() + "/api/v1/ping")
    assert r.status_code == 405
