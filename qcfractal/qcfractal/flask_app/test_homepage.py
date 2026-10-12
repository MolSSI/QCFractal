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

    # Missing files that aren't requested as html (scripts, images, etc) are not found
    r = requests.get(snowflake.get_uri() + "/missing.js", headers={"Accept": "*/*"})
    assert r.status_code == 404


def test_homepage_spa(postgres_server, tmp_path, client_encoding):
    # Serving a single-page app (like the web portal) with client-side routing
    homepage_dir = tmp_path / "homepage"
    (homepage_dir / "assets").mkdir(parents=True)

    with open(homepage_dir / "index.html", "w") as f:
        f.write('<meta property="og:image" content="__SITE_ORIGIN__/og-image.png" />')
    with open(homepage_dir / "assets" / "index-abc123.js", "w") as f:
        f.write("SCRIPT")

    pg_harness = postgres_server.get_new_harness("test_homepage_spa")
    extra_config = {"homepage_directory": str(homepage_dir)}
    snowflake = QCATestingSnowflake(pg_harness, client_encoding, extra_config=extra_config)
    uri = snowflake.get_uri()
    browser_accept = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"

    expected_index = f'<meta property="og:image" content="{uri}/og-image.png" />'

    for path in ("", "/", "/index.html"):
        r = requests.get(uri + path, headers={"Accept": browser_accept})
        assert r.status_code == 200
        assert r.text == expected_index
        assert r.headers["Cache-Control"] == "no-cache"

    # Navigating to a client-side route gets the index
    for path in ("/projects/123", "/users/some.user", "/api_keys"):
        r = requests.get(uri + path, headers={"Accept": browser_accept})
        assert r.status_code == 200
        assert r.text == expected_index

    # Hashed assets can be cached forever
    r = requests.get(uri + "/assets/index-abc123.js", headers={"Accept": "*/*"})
    assert r.status_code == 200
    assert r.text == "SCRIPT"
    assert "immutable" in r.headers["Cache-Control"]

    # Missing assets are not replaced by the index
    r = requests.get(uri + "/assets/index-missing.js", headers={"Accept": "*/*"})
    assert r.status_code == 404

    # Unknown paths under the server's reserved prefixes are never the homepage
    for path in ("/api", "/api/v1/zzz/nonexistent", "/auth/v1/zzz", "/compute/zzz"):
        r = requests.get(uri + path, headers={"Accept": browser_accept})
        assert r.status_code == 404, path
        assert r.headers["Content-Type"] == "application/json"


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
