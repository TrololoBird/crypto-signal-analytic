"""Modern strategy exports."""

from .bos_choch import BOSCHOCHSetup
from .breaker_block import BreakerBlockSetup
from .cvd_divergence import CVDDivergenceSetup
from .ema_bounce import EmaBounceSetup
from .funding_reversal import FundingReversalSetup
from .fvg import FVGSetup
from .hidden_divergence import HiddenDivergenceSetup
from .liquidity_sweep import LiquiditySweepSetup
from .order_block import OrderBlockSetup
from .session_killzone import SessionKillzoneSetup
from .squeeze_setup import SqueezeSetup
from .structure_break_retest import StructureBreakRetestSetup
from .structure_pullback import StructurePullbackSetup
from .turtle_soup import TurtleSoupSetup
from .wick_trap_reversal import WickTrapReversalSetup

STRATEGY_CLASSES = (
    StructurePullbackSetup,
    StructureBreakRetestSetup,
    WickTrapReversalSetup,
    SqueezeSetup,
    EmaBounceSetup,
    FVGSetup,
    OrderBlockSetup,
    LiquiditySweepSetup,
    BOSCHOCHSetup,
    HiddenDivergenceSetup,
    FundingReversalSetup,
    CVDDivergenceSetup,
    SessionKillzoneSetup,
    BreakerBlockSetup,
    TurtleSoupSetup,
)

__all__ = [
    "BOSCHOCHSetup",
    "BreakerBlockSetup",
    "CVDDivergenceSetup",
    "EmaBounceSetup",
    "FundingReversalSetup",
    "FVGSetup",
    "HiddenDivergenceSetup",
    "LiquiditySweepSetup",
    "OrderBlockSetup",
    "SessionKillzoneSetup",
    "SqueezeSetup",
    "STRATEGY_CLASSES",
    "StructureBreakRetestSetup",
    "StructurePullbackSetup",
    "TurtleSoupSetup",
    "WickTrapReversalSetup",
]
