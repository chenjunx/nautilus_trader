"""交易所配置动态生成模块：根据用户指定的主所+副所，动态构建 data/exec client 配置。

支持的主所（必须同时有现货+永续）：BINANCE, GATEIO, OKX
支持的副所（只需要现货）：KRAKEN, BITFINEX, 以及所有主所的现货部分

使用示例:
    config = build_venue_config(main_venue="BINANCE", secondary_venue="KRAKEN")
    # 返回 {data_clients: {...}, exec_clients: {...}, data_factories: {...}, exec_factories: {...}}
"""

from nautilus_trader.adapters.binance import BINANCE
from nautilus_trader.adapters.binance import BinanceDataClientConfig
from nautilus_trader.adapters.binance import BinanceExecClientConfig
from nautilus_trader.adapters.binance import BinanceLiveDataClientFactory
from nautilus_trader.adapters.binance import BinanceLiveExecClientFactory
from nautilus_trader.adapters.binance.common.enums import BinanceAccountType
from nautilus_trader.adapters.binance.common.enums import BinanceEnvironment
from nautilus_trader.adapters.bitfinex import BITFINEX
from nautilus_trader.adapters.bitfinex import BitfinexDataClientConfig
from nautilus_trader.adapters.bitfinex import BitfinexExecClientConfig
from nautilus_trader.adapters.bitfinex import BitfinexInstrumentType
from nautilus_trader.adapters.bitfinex import BitfinexLiveDataClientFactory
from nautilus_trader.adapters.bitfinex import BitfinexLiveExecClientFactory
from nautilus_trader.adapters.gateio import GATEIO
from nautilus_trader.adapters.gateio import GateIoAccountType
from nautilus_trader.adapters.gateio import GateIoDataClientConfig
from nautilus_trader.adapters.gateio import GateIoExecClientConfig
from nautilus_trader.adapters.gateio import GateIoLiveDataClientFactory
from nautilus_trader.adapters.gateio import GateIoLiveExecClientFactory
from nautilus_trader.adapters.kraken import KRAKEN
from nautilus_trader.adapters.kraken import KrakenDataClientConfig
from nautilus_trader.adapters.kraken import KrakenEnvironment
from nautilus_trader.adapters.kraken import KrakenExecClientConfig
from nautilus_trader.adapters.kraken import KrakenLiveDataClientFactory
from nautilus_trader.adapters.kraken import KrakenLiveExecClientFactory
from nautilus_trader.adapters.kraken import KrakenProductType
from nautilus_trader.adapters.okx import OKX
from nautilus_trader.adapters.okx import OKXDataClientConfig
from nautilus_trader.adapters.okx import OKXExecClientConfig
from nautilus_trader.adapters.okx import OKXLiveDataClientFactory
from nautilus_trader.adapters.okx import OKXLiveExecClientFactory
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.core.nautilus_pyo3 import OKXContractType
from nautilus_trader.core.nautilus_pyo3 import OKXEnvironment
from nautilus_trader.core.nautilus_pyo3 import OKXInstrumentType
from nautilus_trader.model.venues import Venue


BINANCE_FUT_KEY = "BINANCE_FUT"
GATEIO_FUT_KEY = "GATEIO_FUT"


def build_venue_config(
    main_venue: str,
    secondary_venue: str,
    binance_environment: BinanceEnvironment = BinanceEnvironment.LIVE,
) -> dict:
    """构建指定主所+副所的完整配置（data/exec clients + factories）。

    Args:
        main_venue: 主所名称（BINANCE, GATEIO, OKX），必须同时有现货+永续
        secondary_venue: 副所名称（KRAKEN, BITFINEX, 或任意主所），只需要现货
        binance_environment: Binance 环境（LIVE 或 TESTNET），只影响 BINANCE 相关配置

    Returns:
        {
            "main_spot_venue": str,           # 主所现货 venue key
            "main_perp_venue": str,           # 主所永续 venue key
            "secondary_spot_venue": str,      # 副所现货 venue key
            "data_clients": dict,             # TradingNodeConfig.data_clients
            "exec_clients": dict,             # TradingNodeConfig.exec_clients
            "data_factories": dict,           # venue -> DataClientFactory
            "exec_factories": dict,           # venue -> ExecClientFactory
        }

    Raises:
        ValueError: 不支持的交易所组合
    """
    main_venue = main_venue.upper()
    secondary_venue = secondary_venue.upper()

    data_clients = {}
    exec_clients = {}
    data_factories = {}
    exec_factories = {}

    main_spot_venue = None
    main_perp_venue = None
    secondary_spot_venue = None

    # 配置主所（现货 + 永续）
    if main_venue == BINANCE:
        main_spot_venue = BINANCE
        main_perp_venue = BINANCE_FUT_KEY

        data_clients[BINANCE] = BinanceDataClientConfig(
            environment=binance_environment,
            account_type=BinanceAccountType.SPOT,
            instrument_provider=InstrumentProviderConfig(load_all=True),
        )
        exec_clients[BINANCE] = BinanceExecClientConfig(
            environment=binance_environment,
            account_type=BinanceAccountType.SPOT,
            instrument_provider=InstrumentProviderConfig(load_all=True),
            max_retries=3,
        )
        data_factories[BINANCE] = BinanceLiveDataClientFactory
        exec_factories[BINANCE] = BinanceLiveExecClientFactory

        data_clients[BINANCE_FUT_KEY] = BinanceDataClientConfig(
            venue=Venue(BINANCE_FUT_KEY),
            environment=binance_environment,
            account_type=BinanceAccountType.USDT_FUTURES,
            instrument_provider=InstrumentProviderConfig(load_all=True),
        )
        exec_clients[BINANCE_FUT_KEY] = BinanceExecClientConfig(
            venue=Venue(BINANCE_FUT_KEY),
            environment=binance_environment,
            account_type=BinanceAccountType.USDT_FUTURES,
            instrument_provider=InstrumentProviderConfig(load_all=True),
            max_retries=3,
        )
        data_factories[BINANCE_FUT_KEY] = BinanceLiveDataClientFactory
        exec_factories[BINANCE_FUT_KEY] = BinanceLiveExecClientFactory

    elif main_venue == GATEIO:
        main_spot_venue = GATEIO
        main_perp_venue = GATEIO_FUT_KEY

        data_clients[GATEIO] = GateIoDataClientConfig(
            instrument_provider=InstrumentProviderConfig(load_all=True),
        )
        exec_clients[GATEIO] = GateIoExecClientConfig(
            instrument_provider=InstrumentProviderConfig(load_all=True),
        )
        data_factories[GATEIO] = GateIoLiveDataClientFactory
        exec_factories[GATEIO] = GateIoLiveExecClientFactory

        data_clients[GATEIO_FUT_KEY] = GateIoDataClientConfig(
            venue=Venue(GATEIO_FUT_KEY),
            account_type=GateIoAccountType.LINEAR,
            instrument_provider=InstrumentProviderConfig(load_all=True),
        )
        exec_clients[GATEIO_FUT_KEY] = GateIoExecClientConfig(
            venue=Venue(GATEIO_FUT_KEY),
            account_type=GateIoAccountType.LINEAR,
            instrument_provider=InstrumentProviderConfig(load_all=True),
        )
        data_factories[GATEIO_FUT_KEY] = GateIoLiveDataClientFactory
        exec_factories[GATEIO_FUT_KEY] = GateIoLiveExecClientFactory

    elif main_venue == OKX:
        main_spot_venue = OKX
        main_perp_venue = OKX  # OKX 现货和永续共用同一个 venue

        data_clients[OKX] = OKXDataClientConfig(
            environment=OKXEnvironment.LIVE,
            instrument_types=(OKXInstrumentType.SPOT, OKXInstrumentType.SWAP),
            contract_types=(OKXContractType.LINEAR,),
            instrument_provider=InstrumentProviderConfig(load_all=True),
        )
        exec_clients[OKX] = OKXExecClientConfig(
            environment=OKXEnvironment.LIVE,
            instrument_types=(OKXInstrumentType.SPOT, OKXInstrumentType.SWAP),
            contract_types=(OKXContractType.LINEAR,),
            instrument_provider=InstrumentProviderConfig(load_all=True),
        )
        data_factories[OKX] = OKXLiveDataClientFactory
        exec_factories[OKX] = OKXLiveExecClientFactory

    else:
        raise ValueError(
            f"不支持的主所: {main_venue}，支持的主所: BINANCE, GATEIO, OKX"
        )

    # 配置副所（只需现货）
    if secondary_venue == KRAKEN:
        secondary_spot_venue = KRAKEN

        data_clients[KRAKEN] = KrakenDataClientConfig(
            environment=KrakenEnvironment.LIVE,
            product_types=(KrakenProductType.SPOT,),
            instrument_provider=InstrumentProviderConfig(load_all=True),
        )
        exec_clients[KRAKEN] = KrakenExecClientConfig(
            environment=KrakenEnvironment.LIVE,
            product_types=(KrakenProductType.SPOT,),
            instrument_provider=InstrumentProviderConfig(load_all=True),
            use_spot_position_reports=True,
            spot_positions_quote_currency="USDT",
        )
        data_factories[KRAKEN] = KrakenLiveDataClientFactory
        exec_factories[KRAKEN] = KrakenLiveExecClientFactory

    elif secondary_venue == BITFINEX:
        secondary_spot_venue = BITFINEX

        data_clients[BITFINEX] = BitfinexDataClientConfig(
            instrument_types=(BitfinexInstrumentType.SPOT, BitfinexInstrumentType.PERPETUAL),
            instrument_provider=InstrumentProviderConfig(load_all=True),
        )
        exec_clients[BITFINEX] = BitfinexExecClientConfig(
            instrument_types=(BitfinexInstrumentType.SPOT, BitfinexInstrumentType.PERPETUAL),
            instrument_provider=InstrumentProviderConfig(load_all=True),
        )
        data_factories[BITFINEX] = BitfinexLiveDataClientFactory
        exec_factories[BITFINEX] = BitfinexLiveExecClientFactory

    elif secondary_venue == BINANCE and main_venue != BINANCE:
        # 副所是 BINANCE 现货（主所不是 BINANCE 时）
        secondary_spot_venue = BINANCE

        data_clients[BINANCE] = BinanceDataClientConfig(
            environment=binance_environment,
            account_type=BinanceAccountType.SPOT,
            instrument_provider=InstrumentProviderConfig(load_all=True),
        )
        exec_clients[BINANCE] = BinanceExecClientConfig(
            environment=binance_environment,
            account_type=BinanceAccountType.SPOT,
            instrument_provider=InstrumentProviderConfig(load_all=True),
            max_retries=3,
        )
        data_factories[BINANCE] = BinanceLiveDataClientFactory
        exec_factories[BINANCE] = BinanceLiveExecClientFactory

    elif secondary_venue == GATEIO and main_venue != GATEIO:
        # 副所是 GATEIO 现货（主所不是 GATEIO 时）
        secondary_spot_venue = GATEIO

        data_clients[GATEIO] = GateIoDataClientConfig(
            instrument_provider=InstrumentProviderConfig(load_all=True),
        )
        exec_clients[GATEIO] = GateIoExecClientConfig(
            instrument_provider=InstrumentProviderConfig(load_all=True),
        )
        data_factories[GATEIO] = GateIoLiveDataClientFactory
        exec_factories[GATEIO] = GateIoLiveExecClientFactory

    elif secondary_venue == OKX and main_venue != OKX:
        # 副所是 OKX 现货（主所不是 OKX 时）
        secondary_spot_venue = OKX

        data_clients[OKX] = OKXDataClientConfig(
            environment=OKXEnvironment.LIVE,
            instrument_types=(OKXInstrumentType.SPOT,),
            instrument_provider=InstrumentProviderConfig(load_all=True),
        )
        exec_clients[OKX] = OKXExecClientConfig(
            environment=OKXEnvironment.LIVE,
            instrument_types=(OKXInstrumentType.SPOT,),
            instrument_provider=InstrumentProviderConfig(load_all=True),
        )
        data_factories[OKX] = OKXLiveDataClientFactory
        exec_factories[OKX] = OKXLiveExecClientFactory

    else:
        raise ValueError(
            f"不支持的副所: {secondary_venue}，或主副所相同。支持的副所: KRAKEN, BITFINEX, BINANCE, GATEIO, OKX"
        )

    return {
        "main_spot_venue": main_spot_venue,
        "main_perp_venue": main_perp_venue,
        "secondary_spot_venue": secondary_spot_venue,
        "data_clients": data_clients,
        "exec_clients": exec_clients,
        "data_factories": data_factories,
        "exec_factories": exec_factories,
    }
