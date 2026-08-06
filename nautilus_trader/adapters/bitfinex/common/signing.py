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
Bitfinex REST/WebSocket request signing.

Bitfinex's v2 API requires HMAC-**SHA384** signatures. `nautilus_pyo3.hmac_signature`
(`crates/cryptography/src/signing.rs`) is hardcoded to HMAC-SHA256 and therefore cannot be
reused here; these functions use the standard library `hmac`/`hashlib` directly instead.

"""

import hashlib
import hmac


def bitfinex_rest_signature(path: str, nonce: str, body: str, api_secret: str) -> str:
    """
    Compute the Bitfinex v2 REST signature for a private request.

    Parameters
    ----------
    path : str
        The request path including the API prefix, e.g. ``"v2/auth/w/order/submit"``.
    nonce : str
        The strictly increasing nonce (microseconds since epoch, as a string).
    body : str
        The raw JSON request body, or ``"{}"`` for requests without a body.
    api_secret : str
        The Bitfinex API secret.

    Returns
    -------
    str
        The hex-encoded HMAC-SHA384 signature.

    """
    signature_string = f"/api/{path}{nonce}{body}"
    return hmac.new(
        api_secret.encode(),
        signature_string.encode(),
        hashlib.sha384,
    ).hexdigest()


def bitfinex_ws_auth_payload(nonce: str, api_key: str, api_secret: str) -> dict[str, object]:
    """
    Compute the Bitfinex private WebSocket authentication payload.

    Sent once after connecting; after which order/wallet/trade/position updates are
    pushed automatically without any per-channel subscription.

    Parameters
    ----------
    nonce : str
        The strictly increasing nonce (microseconds since epoch, as a string).
    api_key : str
        The Bitfinex API key.
    api_secret : str
        The Bitfinex API secret.

    Returns
    -------
    dict[str, object]
        The ``auth`` event payload to send over the WebSocket connection.

    """
    auth_payload = f"AUTH{nonce}"
    signature = hmac.new(
        api_secret.encode(),
        auth_payload.encode(),
        hashlib.sha384,
    ).hexdigest()

    return {
        "event": "auth",
        "apiKey": api_key,
        "authSig": signature,
        "authPayload": auth_payload,
        "authNonce": nonce,
        "dms": 4,
    }
