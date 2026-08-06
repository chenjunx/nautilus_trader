# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------
"""
Gate.io REST/WebSocket request signing.

Gate.io's v4 API requires HMAC-**SHA512** signatures. `nautilus_pyo3.hmac_signature`
(`crates/cryptography/src/signing.rs`) is hardcoded to HMAC-SHA256 and therefore cannot be
reused here; these functions use the standard library `hmac`/`hashlib` directly instead.

"""

import hashlib
import hmac


def gateio_rest_signature(
    method: str,
    path: str,
    query_string: str,
    body: str,
    api_key: str,
    api_secret: str,
    timestamp: str,
) -> dict[str, str]:
    """
    Compute the Gate.io v4 REST signature headers for a private request.

    Parameters
    ----------
    method : str
        The uppercase HTTP method, e.g. ``"GET"``, ``"POST"``, ``"DELETE"``.
    path : str
        The full request path including the API prefix, e.g. ``"/api/v4/spot/orders"``.
    query_string : str
        The URL-encoded query string (without the leading ``?``), or an empty string.
    body : str
        The raw request body, or an empty string for requests without a body.
    api_key : str
        The Gate.io API key.
    api_secret : str
        The Gate.io API secret.
    timestamp : str
        The Unix timestamp (seconds) to sign with, as a string.

    Returns
    -------
    dict[str, str]
        The ``KEY``, ``Timestamp``, and ``SIGN`` headers to attach to the request.

    """
    hashed_payload = hashlib.sha512(body.encode()).hexdigest()
    signature_string = "\n".join(
        [method, path, query_string, hashed_payload, timestamp],
    )
    signature = hmac.new(
        api_secret.encode(),
        signature_string.encode(),
        hashlib.sha512,
    ).hexdigest()

    return {
        "KEY": api_key,
        "Timestamp": timestamp,
        "SIGN": signature,
    }


def gateio_ws_channel_auth(
    channel: str,
    event: str,
    timestamp: int,
    api_key: str,
    api_secret: str,
) -> dict[str, str]:
    """
    Compute the Gate.io private WebSocket channel authentication payload.

    Must be recomputed for every subscribe call (the signed string embeds the
    channel, event, and timestamp), it cannot be cached and reused globally.

    Parameters
    ----------
    channel : str
        The channel name, e.g. ``"spot.orders"``.
    event : str
        The event name, e.g. ``"subscribe"``.
    timestamp : int
        The Unix timestamp (seconds) to sign with.
    api_key : str
        The Gate.io API key.
    api_secret : str
        The Gate.io API secret.

    Returns
    -------
    dict[str, str]
        The ``method``, ``KEY``, and ``SIGN`` fields for the ``auth`` object.

    """
    signature_string = f"channel={channel}&event={event}&time={timestamp}"
    signature = hmac.new(
        api_secret.encode(),
        signature_string.encode(),
        hashlib.sha512,
    ).hexdigest()

    return {
        "method": "api_key",
        "KEY": api_key,
        "SIGN": signature,
    }
