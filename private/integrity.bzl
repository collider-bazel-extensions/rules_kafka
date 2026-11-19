"""sha512 hex digest -> Subresource Integrity string, for
repository_ctx.download_and_extract(integrity = ...). Apache publishes
sha512 (not sha256) for its release archives."""

_HEX = "0123456789abcdef"
_B64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

def sha512_integrity(hexdigest):
    """"sha512-<base64>" for a 128-digit hex sha512 (any case, spaces ignored)."""
    h = hexdigest.lower().replace(" ", "")
    if len(h) != 128 or [c for c in h.elems() if c not in _HEX]:
        fail("rules_kafka: sha512 must be 128 hex digits, got \"{}\"".format(hexdigest))
    data = [_HEX.index(h[2 * i]) * 16 + _HEX.index(h[2 * i + 1]) for i in range(64)]
    out = []
    for i in range(0, 64, 3):
        chunk = data[i:i + 3]
        n = len(chunk)
        b = chunk + [0] * (3 - n)
        v = (b[0] << 16) | (b[1] << 8) | b[2]
        chars = [_B64[(v >> 18) & 63], _B64[(v >> 12) & 63], _B64[(v >> 6) & 63], _B64[v & 63]]
        if n < 3:
            chars = chars[:n + 1] + ["="] * (3 - n)
        out.extend(chars)
    return "sha512-" + "".join(out)
