from nautilus_trader.adapters.binance import BINANCE
from nautilus_trader.adapters.binance import BinanceDataClientConfig
from nautilus_trader.adapters.binance import BinanceLiveDataClientFactory
from nautilus_trader.adapters.binance.common.enums import BinanceAccountType
from nautilus_trader.adapters.binance.common.enums import BinanceEnvironment
from nautilus_trader.adapters.bitfinex import BITFINEX
from nautilus_trader.adapters.bitfinex import BitfinexDataClientConfig
from nautilus_trader.adapters.bitfinex import BitfinexInstrumentType
from nautilus_trader.adapters.bitfinex import BitfinexLiveDataClientFactory
from nautilus_trader.adapters.gateio import GATEIO
from nautilus_trader.adapters.gateio import GateIoAccountType
from nautilus_trader.adapters.gateio import GateIoDataClientConfig
from nautilus_trader.adapters.gateio import GateIoLiveDataClientFactory
from nautilus_trader.adapters.kraken import KRAKEN
from nautilus_trader.adapters.kraken import KrakenDataClientConfig
from nautilus_trader.adapters.kraken import KrakenEnvironment
from nautilus_trader.adapters.kraken import KrakenLiveDataClientFactory
from nautilus_trader.adapters.kraken import KrakenProductType
from nautilus_trader.adapters.okx import OKX
from nautilus_trader.adapters.okx import OKXDataClientConfig
from nautilus_trader.adapters.okx import OKXLiveDataClientFactory
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.core.nautilus_pyo3 import OKXContractType
from nautilus_trader.core.nautilus_pyo3 import OKXEnvironment
from nautilus_trader.core.nautilus_pyo3 import OKXInstrumentType
from nautilus_trader.model.venues import Venue


# Binance futures 使用独立 venue key，便于与现货区分
BINANCE_FUT_KEY = "BINANCE_FUT"
# Gate.io 永续同理，使用独立 venue key 与现货区分（二者共用同一交易所账号，
# 适配器层已支持通过 venue+name 拆分成两个独立 client，避免 client_id 冲突）
GATEIO_FUT_KEY = "GATEIO_FUT"

# 交易所配置总表：新增/删除一个所，只需在此增删一条记录。
# roles: "main_spot" | "main_perp" | "secondary"
#   一个所若同时具备 main_spot + main_perp（USDT 现货 + USDT 永续）即为主所；
#   只有 secondary 角色的所为副所，只交易现货，不参与开仓/永续对冲。
# instrument_venue: 该 client 加载出来的 instrument 实际挂的 venue（默认等于 key）。
# Binance/Gate.io 永续都通过 venue= 参数覆盖，与现货 client 拆成两个独立 key/venue，
# 避免同一交易所现货+永续 client_id 冲突。
VENUE_REGISTRY: list[dict] = [
    {
        "key": BINANCE,
        "roles": {"main_spot"},
        "config": lambda: BinanceDataClientConfig(
            environment=BinanceEnvironment.LIVE,
            account_type=BinanceAccountType.SPOT,
            instrument_provider=InstrumentProviderConfig(load_all=True),
        ),
        "factory": BinanceLiveDataClientFactory,
        "default_fee": 0.00075,   # BNB 折扣后
    },
    {
        "key": BINANCE_FUT_KEY,
        "roles": {"main_perp"},
        "config": lambda: BinanceDataClientConfig(
            venue=Venue(BINANCE_FUT_KEY),
            environment=BinanceEnvironment.LIVE,
            account_type=BinanceAccountType.USDT_FUTURES,
            instrument_provider=InstrumentProviderConfig(load_all=True),
        ),
        "factory": BinanceLiveDataClientFactory,
        "default_fee": None,
    },
    {
        "key": KRAKEN,
        "roles": {"secondary"},
        "config": lambda: KrakenDataClientConfig(
            environment=KrakenEnvironment.LIVE,
            product_types=(KrakenProductType.SPOT,),
            instrument_provider=InstrumentProviderConfig(load_all=True),
        ),
        "factory": KrakenLiveDataClientFactory,
        "default_fee": 0.00050,   # 静态兜底值（30天量 >$50k 档位）；配置 KRAKEN_SPOT_API_KEY/SECRET
                                   # 后启动时会用账户真实费率覆盖，见 on_start() 里的拉取逻辑
    },
    {
        # Bitfinex 现货与 USDT 永续共用同一个 client/venue key（不像 Binance/Gate.io 需要
        # 拆成独立账户类型/venue），通过 instrument_types 元组同时加载两类合约。
        "key": BITFINEX,
        "roles": {"secondary", "main_perp"},
        "config": lambda: BitfinexDataClientConfig(
            instrument_types=(BitfinexInstrumentType.SPOT, BitfinexInstrumentType.PERPETUAL),
            instrument_provider=InstrumentProviderConfig(load_all=True),
        ),
        "factory": BitfinexLiveDataClientFactory,
        "default_fee": 0.00200,   # Bitfinex 现货 taker 基础费率（未开 LEO 折扣/未按 30 天量升档时的默认档位），
                                   # 需按实际账户等级核实；该适配器为纯公开行情接口，无法像 Kraken 那样拉取账户真实费率。
                                   # 衍生品目前 0 手续费（2025-12-17 起），已直接编码在 CryptoPerpetual 上，
                                   # 此处 default_fee 只影响现货侧
    },
    {
        "key": GATEIO,
        "roles": {"main_spot"},
        "config": lambda: GateIoDataClientConfig(
            instrument_provider=InstrumentProviderConfig(load_all=True),
        ),
        "factory": GateIoLiveDataClientFactory,
        "default_fee": 0.00080,
    },
    {
        "key": GATEIO_FUT_KEY,
        "roles": {"main_perp"},
        "config": lambda: GateIoDataClientConfig(
            venue=Venue(GATEIO_FUT_KEY),
            account_type=GateIoAccountType.LINEAR,
            instrument_provider=InstrumentProviderConfig(load_all=True),
        ),
        "factory": GateIoLiveDataClientFactory,
        "default_fee": 0.00050,   # Gate.io USDT 永续 taker 费率，需按实际账户等级核实
    },
    {
        # OKX 现货与 USDT 永续可共用同一个 client 一起加载（不像 Binance/Gate.io 需要
        # 独立账户类型/venue 拆分），因此单条记录即可同时具备 main_spot + main_perp。
        "key": OKX,
        "roles": {"main_spot", "main_perp"},
        "config": lambda: OKXDataClientConfig(
            environment=OKXEnvironment.LIVE,
            instrument_types=(OKXInstrumentType.SPOT, OKXInstrumentType.SWAP),
            contract_types=(OKXContractType.LINEAR,),   # LINEAR = USDT/USDC 保证金永续，quote=="USDT" 过滤已在选币逻辑里处理
            instrument_provider=InstrumentProviderConfig(load_all=True),
        ),
        "factory": OKXLiveDataClientFactory,
        "default_fee": 0.00080,   # OKX 现货 taker，需按实际账户等级核实
    },
]

# 主所（有永续 + 现货）
MAIN_SPOT_VENUES = {str(v.get("instrument_venue", v["key"])) for v in VENUE_REGISTRY if "main_spot" in v["roles"]}
MAIN_PERP_VENUES = {str(v.get("instrument_venue", v["key"])) for v in VENUE_REGISTRY if "main_perp" in v["roles"]}

# 副所（只交易现货，不参与开仓/永续对冲）
SECONDARY_VENUES = {str(v.get("instrument_venue", v["key"])) for v in VENUE_REGISTRY if "secondary" in v["roles"]}

# 黑名单：流动性过高，套利竞争激烈
BLACKLIST = {"BTC", "ETH", "SOL", "XRP", "BNB"}

# 各所折扣后 taker 费率默认值
DEFAULT_FEES: dict[str, float] = {
    str(v["key"]): v["default_fee"] for v in VENUE_REGISTRY if v["default_fee"] is not None
}


def parse_fees_arg(fees_str: str) -> dict[str, float]:
    """
    Parse --fees argument like "BINANCE=0.001,KRAKEN=0.002"
    into {venue_name: fee_rate}. 未指定的所使用 DEFAULT_FEES。
    """
    result = dict(DEFAULT_FEES)  # 以各所默认值为基础
    if not fees_str:
        return result
    for part in fees_str.split(","):
        part = part.strip()
        if "=" not in part:
            continue
        venue, rate = part.split("=", 1)
        venue = venue.strip().upper()
        if venue in result:
            result[venue] = float(rate.strip())
    return result
