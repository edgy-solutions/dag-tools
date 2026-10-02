"""Injecting a CA bundle for self-signed TLS endpoints.

The chart had no way to do it at all -- no volumes, no volumeMounts, no
trust-store env vars anywhere in the restate-worker or central-gateway
Deployments -- so a handler pointed at an endpoint behind a private CA
failed on every call with no in-chart workaround.

The naive fix (one env var naming the operator's cert) is wrong twice over,
and both halves are measured here rather than asserted from memory:

  * The code uses BOTH `requests` and `httpx`, and they disagree about
    which env var names a CA bundle. `requests` honours REQUESTS_CA_BUNDLE
    and ignores SSL_CERT_FILE; `httpx` honours SSL_CERT_FILE and ignores
    REQUESTS_CA_BUNDLE. Pick one and half the call sites still reject the
    endpoint -- including the worker's own Restate registration, which goes
    through httpx, so the worker would never serve anything.

  * Those env vars REPLACE the default trust store rather than extending
    it, so naming a private CA directly breaks every PUBLIC endpoint. The
    chart therefore concatenates the image's public roots with the
    operator's certs in an init container and names the merged file.

`TestWhatTheClientsActuallyHonour` is the canary for the first fact: if a
future httpx or requests changes its mind, the chart's rationale changes
with it and these tests say so.
"""
import datetime
import os
import pathlib
import shutil
import ssl
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

yaml = pytest.importorskip("yaml")
pytest.importorskip("cryptography")

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID


CHART = pathlib.Path(__file__).parent.parent / "helm" / "dag-tools"

# Every render needs this: the chart fails template rendering outright when
# neither an in-chart Restate nor externalRestate.adminUrl is configured.
BASE = ["--set", "restateServer.enabled=true"]


# ---------------------------------------------------------------------------
# A real private CA and a real TLS endpoint
# ---------------------------------------------------------------------------


def _self_signed(tmp_path):
    tmp_path = pathlib.Path(tmp_path)
    tmp_path.mkdir(parents=True, exist_ok=True)
    """A private CA plus a server cert it signed, both on disk."""
    now = datetime.datetime.now(datetime.timezone.utc)

    def key():
        return rsa.generate_private_key(public_exponent=65537, key_size=2048)

    ca_key = key()
    ca_name = x509.Name(
        [x509.NameAttribute(NameOID.COMMON_NAME, "dag-tools-test-ca")]
    )
    ca = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(
            x509.BasicConstraints(ca=True, path_length=None), critical=True
        )
        .sign(ca_key, hashes.SHA256())
    )

    srv_key = key()
    srv = (
        x509.CertificateBuilder()
        .subject_name(
            x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
        )
        .issuer_name(ca_name)
        .public_key(srv_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(
            x509.SubjectAlternativeName([x509.DNSName("localhost")]),
            critical=False,
        )
        .sign(ca_key, hashes.SHA256())
    )

    ca_path = tmp_path / "ca.crt"
    ca_path.write_bytes(ca.public_bytes(serialization.Encoding.PEM))
    srv_path = tmp_path / "srv.pem"
    srv_path.write_bytes(
        srv.public_bytes(serialization.Encoding.PEM)
        + srv_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
    )
    return ca_path, srv_path


@pytest.fixture(scope="module")
def tls_endpoint(tmp_path_factory):
    """An HTTPS server whose cert only the private CA vouches for."""
    tmp_path = tmp_path_factory.mktemp("ca")
    ca_path, srv_path = _self_signed(tmp_path)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, *args):
            pass

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(str(srv_path))
    httpd = HTTPServer(("127.0.0.1", 0), Handler)
    httpd.socket = ctx.wrap_socket(httpd.socket, server_side=True)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"https://localhost:{httpd.server_address[1]}/", ca_path, tmp_path
    finally:
        httpd.shutdown()


# A fetch in a clean interpreter: these libraries read the trust-store env
# vars once, at import or first use, so setting them in-process after the
# fact proves nothing.
_FETCH = r"""
import sys
lib, url = sys.argv[1], sys.argv[2]
try:
    if lib == "requests":
        import requests; requests.get(url, timeout=15)
    else:
        import httpx; httpx.get(url, timeout=15)
    print("TRUSTED")
except Exception as exc:
    print("REJECTED:" + type(exc).__name__)
"""

_TRUST_VARS = ("SSL_CERT_FILE", "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE")


def _fetch(lib, url, **trust):
    env = {k: v for k, v in os.environ.items() if k not in _TRUST_VARS}
    env.update({k: str(v) for k, v in trust.items()})
    result = subprocess.run(
        [sys.executable, "-c", _FETCH, lib, url],
        capture_output=True,
        text=True,
        env=env,
    )
    return result.stdout.strip() or f"ERROR:{result.stderr.strip()[:200]}"


class TestWhatTheClientsActuallyHonour:
    """Why the chart sets BOTH env vars and merges rather than replaces."""

    def test_an_unknown_ca_is_rejected_by_both(self, tls_endpoint):
        """Guards the guard. If the endpoint were trusted by default,
        every test below would pass vacuously."""
        url, _, _ = tls_endpoint
        assert _fetch("requests", url).startswith("REJECTED")
        assert _fetch("httpx", url).startswith("REJECTED")

    def test_requests_honours_REQUESTS_CA_BUNDLE_and_ignores_SSL_CERT_FILE(
        self, tls_endpoint
    ):
        url, ca, _ = tls_endpoint
        assert _fetch("requests", url, REQUESTS_CA_BUNDLE=ca) == "TRUSTED"
        assert _fetch("requests", url, SSL_CERT_FILE=ca).startswith("REJECTED")

    def test_httpx_honours_SSL_CERT_FILE_and_ignores_REQUESTS_CA_BUNDLE(
        self, tls_endpoint
    ):
        """This is the asymmetry that makes one env var insufficient. The
        worker's Restate self-registration is httpx, so a REQUESTS_CA_BUNDLE
        -only chart would leave the worker unable to register at all."""
        url, ca, _ = tls_endpoint
        assert _fetch("httpx", url, SSL_CERT_FILE=ca) == "TRUSTED"
        assert _fetch("httpx", url, REQUESTS_CA_BUNDLE=ca).startswith("REJECTED")

    def test_naming_the_private_ca_alone_breaks_public_tls(self, tls_endpoint):
        """Why an init container merges instead of the chart just mounting
        the cert and naming it. Uses a loopback endpoint signed by a
        DIFFERENT private CA as the stand-in for "a host this bundle does
        not vouch for" -- no network needed, same verification path."""
        url, ca, tmp_path = tls_endpoint
        other_ca, _ = _self_signed(tmp_path / "other")

        for lib in ("requests", "httpx"):
            assert _fetch(
                lib, url, SSL_CERT_FILE=other_ca, REQUESTS_CA_BUNDLE=other_ca
            ).startswith("REJECTED"), (
                f"{lib} trusted an endpoint the named bundle does not vouch "
                f"for -- the env var no longer replaces the trust store, so "
                f"the chart's merge step may be reconsidered"
            )

    def test_the_merged_bundle_trusts_both(self, tls_endpoint):
        """What the init container produces: public roots plus the private
        CA. Both the private endpoint and one signed by an unrelated
        private CA resolve correctly -- trusted and rejected respectively --
        which is exactly what 'extend, do not replace' means."""
        url, ca, tmp_path = tls_endpoint
        certifi = pytest.importorskip("certifi")

        merged = tmp_path / "merged.crt"
        merged.write_bytes(
            pathlib.Path(certifi.where()).read_bytes() + b"\n" + ca.read_bytes()
        )

        for lib in ("requests", "httpx"):
            assert (
                _fetch(lib, url, SSL_CERT_FILE=merged, REQUESTS_CA_BUNDLE=merged)
                == "TRUSTED"
            ), f"{lib} rejected the private CA inside the merged bundle"

        # The merge must not be a blanket "trust anything" either.
        rogue_ca, rogue_srv = _self_signed(tmp_path / "rogue")
        assert rogue_ca.exists() and rogue_srv.exists()
        assert _fetch(
            "httpx", url, SSL_CERT_FILE=rogue_ca, REQUESTS_CA_BUNDLE=rogue_ca
        ).startswith("REJECTED")


# ---------------------------------------------------------------------------
# What the chart renders
# ---------------------------------------------------------------------------

helm = shutil.which("helm")
requires_helm = pytest.mark.skipif(
    helm is None,
    reason=(
        "helm is not installed, so chart rendering cannot be checked. CI "
        "installs it (see .github/workflows/test.yml) -- a skip here means "
        "the chart went unverified."
    ),
)


def _render(*overrides, expect_failure=False):
    result = subprocess.run(
        [helm, "template", "t", str(CHART), *BASE, *overrides],
        capture_output=True,
        text=True,
    )
    if expect_failure:
        assert result.returncode != 0, f"expected a render failure:\n{result.stdout}"
        return result.stderr
    assert result.returncode == 0, result.stderr
    return [d for d in yaml.safe_load_all(result.stdout) if d]


def _deployment(docs, name):
    for doc in docs:
        if doc.get("kind") == "Deployment" and doc["metadata"]["name"] == name:
            return doc
    raise AssertionError(f"no Deployment named {name} in {[d.get('kind') for d in docs]}")


def _container(dep, name):
    pod = dep["spec"]["template"]["spec"]
    for c in pod["containers"]:
        if c["name"] == name:
            return c
    raise AssertionError(f"no container named {name}")


def _env(container):
    return {e["name"]: e.get("value") for e in container.get("env", [])}


COMPONENTS = [
    ("restateWorker", "t-restate-worker", "restate-worker"),
    ("centralGateway", "t-central-gateway", "central-gateway"),
]


@requires_helm
@pytest.mark.parametrize("values_key,dep_name,container_name", COMPONENTS)
class TestRendering:
    def test_off_by_default_nothing_is_added(
        self, values_key, dep_name, container_name
    ):
        """Turning this on changes the podspec. It must not change for
        anyone who has not asked for it, or every consumer gets a new init
        container and a rolling restart on upgrade."""
        dep = _deployment(_render(), dep_name)
        pod = dep["spec"]["template"]["spec"]
        assert "initContainers" not in pod
        assert "volumes" not in pod
        container = _container(dep, container_name)
        assert "volumeMounts" not in container
        assert not (set(_TRUST_VARS) & set(_env(container)))

    def test_enabling_sets_both_env_vars(self, values_key, dep_name, container_name):
        """The measured asymmetry: requests needs REQUESTS_CA_BUNDLE, httpx
        needs SSL_CERT_FILE. Either one alone leaves half the call sites
        rejecting the endpoint."""
        dep = _deployment(
            _render(
                "--set", f"{values_key}.caCerts.enabled=true",
                "--set", f"{values_key}.caCerts.secretName=corp-ca",
            ),
            dep_name,
        )
        env = _env(_container(dep, container_name))
        assert "SSL_CERT_FILE" in env, "httpx call sites would still reject"
        assert "REQUESTS_CA_BUNDLE" in env, "requests call sites would still reject"
        assert env["SSL_CERT_FILE"] == env["REQUESTS_CA_BUNDLE"]

    def test_the_env_vars_name_the_MERGED_bundle_not_the_mounted_secret(
        self, values_key, dep_name, container_name
    ):
        """The one failure this whole design exists to prevent: naming the
        operator's cert directly makes every public endpoint fail, which
        looks like an unrelated outage somewhere else in the pipeline."""
        dep = _deployment(
            _render(
                "--set", f"{values_key}.caCerts.enabled=true",
                "--set", f"{values_key}.caCerts.secretName=corp-ca",
            ),
            dep_name,
        )
        pod = dep["spec"]["template"]["spec"]
        bundle = _env(_container(dep, container_name))["SSL_CERT_FILE"]

        (init,) = [
            c for c in pod["initContainers"] if c["name"] == "ca-certs-merge"
        ]
        source_mount = [
            m for m in init["volumeMounts"] if m["name"] == "ca-certs-source"
        ][0]["mountPath"]
        assert not bundle.startswith(source_mount + "/"), (
            f"the trust-store env vars point into the mounted secret "
            f"({bundle}); they must name the merged bundle the init "
            f"container writes, or public TLS breaks"
        )
        # And the init container writes exactly that path.
        assert bundle in init["args"][0]

    def test_the_merge_keeps_the_public_roots(
        self, values_key, dep_name, container_name
    ):
        """Measured: a bundle holding only the private CA rejects public
        endpoints. The init container must concatenate, and the system
        bundle has to be one of the inputs."""
        dep = _deployment(
            _render(
                "--set", f"{values_key}.caCerts.enabled=true",
                "--set", f"{values_key}.caCerts.secretName=corp-ca",
            ),
            dep_name,
        )
        script = [
            c
            for c in dep["spec"]["template"]["spec"]["initContainers"]
            if c["name"] == "ca-certs-merge"
        ][0]["args"][0]
        assert "/etc/ssl/certs/ca-certificates.crt" in script
        assert "certifi.where()" in script, (
            "requests and httpx both default to certifi's bundle, so "
            "omitting it can narrow trust relative to not setting the env "
            "vars at all"
        )

    def test_the_application_container_mounts_the_bundle(
        self, values_key, dep_name, container_name
    ):
        """The init container writing the file is useless if the app
        container cannot see the emptyDir."""
        dep = _deployment(
            _render(
                "--set", f"{values_key}.caCerts.enabled=true",
                "--set", f"{values_key}.caCerts.secretName=corp-ca",
            ),
            dep_name,
        )
        container = _container(dep, container_name)
        bundle = _env(container)["SSL_CERT_FILE"]
        mounts = {m["mountPath"] for m in container["volumeMounts"]}
        assert any(
            bundle.startswith(m.rstrip("/") + "/") for m in mounts
        ), f"{bundle} is not under any mount of the app container: {mounts}"

    def test_the_init_container_fails_on_an_empty_mount(
        self, values_key, dep_name, container_name
    ):
        """A pod that starts with an unchanged bundle looks configured and
        still rejects the endpoint. CrashLoopBackOff naming the empty mount
        is the cheaper failure."""
        dep = _deployment(
            _render(
                "--set", f"{values_key}.caCerts.enabled=true",
                "--set", f"{values_key}.caCerts.secretName=corp-ca",
            ),
            dep_name,
        )
        script = [
            c
            for c in dep["spec"]["template"]["spec"]["initContainers"]
            if c["name"] == "ca-certs-merge"
        ][0]["args"][0]
        assert "exit 1" in script

    def test_a_secret_source_is_mounted_read_only(
        self, values_key, dep_name, container_name
    ):
        dep = _deployment(
            _render(
                "--set", f"{values_key}.caCerts.enabled=true",
                "--set", f"{values_key}.caCerts.secretName=corp-ca",
            ),
            dep_name,
        )
        pod = dep["spec"]["template"]["spec"]
        (vol,) = [v for v in pod["volumes"] if v["name"] == "ca-certs-source"]
        assert vol["secret"]["secretName"] == "corp-ca"

    def test_a_configmap_source_is_accepted(
        self, values_key, dep_name, container_name
    ):
        dep = _deployment(
            _render(
                "--set", f"{values_key}.caCerts.enabled=true",
                "--set", f"{values_key}.caCerts.configMapName=corp-ca",
            ),
            dep_name,
        )
        pod = dep["spec"]["template"]["spec"]
        (vol,) = [v for v in pod["volumes"] if v["name"] == "ca-certs-source"]
        assert vol["configMap"]["name"] == "corp-ca"

    def test_enabled_with_no_source_is_refused_by_name(
        self, values_key, dep_name, container_name
    ):
        """Rendering an empty mount would defer the failure to a running
        pod. The message has to name the component, because the two
        components have separate blocks."""
        stderr = _render(
            "--set", f"{values_key}.caCerts.enabled=true", expect_failure=True
        )
        assert container_name in stderr, stderr
        assert "secretName" in stderr

    def test_two_sources_are_refused(self, values_key, dep_name, container_name):
        """Only one directory is mounted, so a second source would be
        silently dropped -- and the operator would be looking at the wrong
        cert while debugging."""
        stderr = _render(
            "--set", f"{values_key}.caCerts.enabled=true",
            "--set", f"{values_key}.caCerts.secretName=corp-ca",
            "--set", f"{values_key}.caCerts.configMapName=corp-ca",
            expect_failure=True,
        )
        assert "exactly one" in stderr, stderr


@requires_helm
def test_inline_certs_render_a_configmap(tmp_path):
    """The common case: one corporate root, pasted into values. A root CA
    is what the endpoint already presents to every client, so a ConfigMap
    is the right object -- but the PEM's newlines have to survive into it
    as valid YAML."""
    ca_path, _ = _self_signed(tmp_path)
    docs = _render(
        "--set", "restateWorker.caCerts.enabled=true",
        "--set-file", f"restateWorker.caCerts.inline.corp-root\\.crt={ca_path.as_posix()}",
    )
    maps = [
        d
        for d in docs
        if d.get("kind") == "ConfigMap"
        and d["metadata"]["name"] == "t-restate-worker-ca-certs"
    ]
    assert maps, [d["metadata"]["name"] for d in docs if d.get("kind") == "ConfigMap"]
    pem = maps[0]["data"]["corp-root.crt"]
    assert "-----BEGIN CERTIFICATE-----" in pem
    assert "-----END CERTIFICATE-----" in pem
    # Round-trips as a real certificate, not just as text that looks like one.
    assert x509.load_pem_x509_certificate(pem.encode())

    dep = _deployment(docs, "t-restate-worker")
    (vol,) = [
        v
        for v in dep["spec"]["template"]["spec"]["volumes"]
        if v["name"] == "ca-certs-source"
    ]
    assert vol["configMap"]["name"] == "t-restate-worker-ca-certs"


@requires_helm
def test_the_default_chart_still_renders():
    """Guards the guard for everything above: these helpers are included
    unconditionally, so a template error in them would break every install
    whether or not caCerts is used."""
    docs = _render()
    assert _deployment(docs, "t-restate-worker")
    assert _deployment(docs, "t-central-gateway")
