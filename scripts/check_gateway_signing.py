#!/usr/bin/env python3
"""Does THIS image's token library verify a signed token? Exit 0 if it does.

Run inside the built central-gateway image. The gateway is configured for an asymmetric
algorithm, and those live in PyJWT's optional `crypto` extra: installed without it, `import
jwt` still succeeds, the container still starts and passes its healthcheck, and only the HMAC
algorithms register -- so every token is refused as invalid whatever its signature, and the
library's own explanation (`RS256 requires 'cryptography' to be installed`) is swallowed by the
verifier's `except Exception`. Nothing short of presenting a signed token to the image can see
that state, which is why this check exists and why it runs against the image rather than
against a development environment.

It needs no network, no key material and no credential: the key pair is generated in memory
per run, the token is minted and verified in this process, and nothing is written anywhere.

Both verification paths the gateway can take are exercised, because they fail differently:
resolving the key from a JWKS document raises `MissingCryptographyError` while a static PEM
raises `InvalidAlgorithmError`, and a check covering one would pass on an image broken for the
other. Every problem is reported at once -- whoever is fixing the image wants the list.

Exit 0: a signed token verifies. Exit 1: it does not, and the reason is printed.
"""
import json
import sys
import time
from typing import Any, Dict, List

# Protocol vocabulary, as the HTTP statuses in the API-call plan are: the name of a signature
# algorithm, not a name from any deployment. The suite holds this to the gateway's own
# configured default, which is the one place that default is allowed to live.
ALGORITHM = "RS256"
ISSUER = "https://signing-check.invalid/realms/check"
AUDIENCE = "gateway-signing-check"
_OPTIONS: Dict[str, Any] = {"verify_aud": True, "require": ["exp", "iss", "aud"]}


def problems() -> List[str]:
    """Everything wrong with this image's ability to verify a signature. Empty is a pass."""
    found: List[str] = []
    try:
        import jwt
        import jwt.algorithms
    except Exception as exc:  # noqa: BLE001
        return ["the token library does not import at all: %s: %s" % (type(exc).__name__, exc)]

    available = set(jwt.algorithms.get_default_algorithms())
    shown = ", ".join(sorted(available)) or "none"

    # The general statement, independent of which algorithm a deployment configures: the
    # library declares which of its algorithms need `cryptography`, and in an image built
    # without the extra every single one of them is missing.
    needs_crypto = tuple(getattr(jwt.algorithms, "requires_cryptography", ()) or ())
    missing = sorted(a for a in needs_crypto if a not in available)
    if missing and len(missing) == len(needs_crypto):
        found.append(
            "this library registers NO signature algorithms: it declares %d of them as "
            "requiring cryptography (%s) and not one is available. Available here: %s"
            % (len(needs_crypto), ", ".join(missing), shown)
        )
    elif missing:
        found.append("unavailable signature algorithms: %s. Available here: %s" % (", ".join(missing), shown))
    if ALGORITHM not in available:
        found.append("%s is not among this library's algorithms. Available here: %s" % (ALGORITHM, shown))

    try:
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
    except Exception as exc:  # noqa: BLE001
        found.append(
            "cryptography is not installed in this image, so no signature can be checked here: "
            "%s: %s" % (type(exc).__name__, exc)
        )
        return found

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    public_pem = key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    claims = {
        "iss": ISSUER,
        "aud": AUDIENCE,
        "sub": "signing-check",
        "iat": int(time.time()),
        "exp": int(time.time()) + 300,
    }
    try:
        token = jwt.encode(claims, private_pem, algorithm=ALGORITHM)
    except Exception as exc:  # noqa: BLE001
        found.append(
            "a %s token cannot even be minted here: %s: %s" % (ALGORITHM, type(exc).__name__, exc)
        )
        return found

    def _verified(label: str, signing_key: Any) -> None:
        try:
            got = jwt.decode(
                token, signing_key, algorithms=[ALGORITHM], audience=AUDIENCE, options=_OPTIONS
            )
        except Exception as exc:  # noqa: BLE001
            found.append("%s: %s: %s" % (label, type(exc).__name__, exc))
            return
        if got.get("iss") != ISSUER or got.get("aud") != AUDIENCE:
            found.append("%s: the token verified but came back with other claims" % label)

    _verified("a signed token does not verify against a static public key", public_pem)

    try:
        document = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(key.public_key()))
        document.setdefault("alg", ALGORITHM)
        document.setdefault("kid", "signing-check")
        resolved = jwt.PyJWK(document).key
    except Exception as exc:  # noqa: BLE001
        found.append(
            "a key cannot be resolved from a JWKS document, which is how this gateway resolves "
            "one: %s: %s" % (type(exc).__name__, exc)
        )
    else:
        _verified("a signed token does not verify against a key resolved from a JWKS document", resolved)

    return found


def main() -> int:
    found = problems()
    if not found:
        print("gateway signing check: a signed %s token verifies in this image" % ALGORITHM)
        return 0
    print("gateway signing check FAILED -- this image cannot verify a signed token:", file=sys.stderr)
    for problem in found:
        print("  - %s" % problem, file=sys.stderr)
    print(
        "\nInstall the token library with its signature support: `pyjwt[crypto]` in\n"
        "dag_tools/central_gateway/Dockerfile (that list is the image's dependency manifest;\n"
        "the project file's extras govern pip installs only). Left as it is, the gateway starts\n"
        "clean and refuses every token as invalid.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
