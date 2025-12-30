"""
🏛️ Institutional Commodities Analytics Platform v6.1
Integrated Portfolio Analytics • Advanced GARCH & Regime Detection • Machine Learning • Professional Reporting
Streamlit Cloud Optimized with Superior Architecture & Performance
"""

# =============================================================================
# BUILD / VERSION
# =============================================================================
__ICD_BUILD__ = "v7.3.8_DEPLOY_VERIFY"



import os
import math
import warnings
import textwrap
import json
import hashlib
import traceback
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Tuple, List, Union, Callable
from dataclasses import dataclass, field, asdict
from functools import lru_cache, wraps
from concurrent.futures import ThreadPoolExecutor, as_completed
from enum import Enum
from pathlib import Path
import pickle

import numpy as np
import pandas as pd
import streamlit as st
import yfinance as yf

# -----------------------------------------------------------------------------
# yfinance download compatibility helper (Streamlit Cloud safe)
# -----------------------------------------------------------------------------
def yf_download_safe(params: Dict[str, Any]) -> pd.DataFrame:
    """Call yfinance.download with fallbacks for version/arg compatibility."""
    try:
        return yf.download(**params)
    except TypeError:
        # Some yfinance versions don't accept these args
        p = dict(params)
        p.pop("threads", None)
        p.pop("timeout", None)
        # Backward compatibility: if someone accidentally uses 'symbol'
        if "tickers" not in p and "symbol" in p:
            p["tickers"] = p.pop("symbol")
        return yf.download(**p)
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from scipy import stats, optimize, signal
# Optional dependency (used only for some diagnostic plots)
try:
    import seaborn as sns  # type: ignore
except Exception:
    sns = None
from io import BytesIO, StringIO
import base64


# =============================================================================
# EXCEL EXPORT (CLOUD-SAFE ENGINE FALLBACK)
# =============================================================================
def icd_safe_excel_writer(buffer_obj):
    """
    Create a pandas ExcelWriter with a robust engine fallback.

    Streamlit Cloud deployments sometimes omit optional Excel dependencies.
    This helper tries `openpyxl` first (common default), then `xlsxwriter`.
    If neither engine is available, returns (None, None) and the caller can
    disable Excel export gracefully instead of crashing the app.
    """
    # Try openpyxl (preferred for .xlsx read/write compatibility)
    try:
        import openpyxl  # noqa: F401
        return pd.ExcelWriter(buffer_obj, engine="openpyxl"), "openpyxl"
    except Exception:
        pass

    # Try xlsxwriter (fast writer-only engine; great fallback on Cloud)
    try:
        import xlsxwriter  # noqa: F401
        return pd.ExcelWriter(buffer_obj, engine="xlsxwriter"), "xlsxwriter"
    except Exception:
        pass

    return None, None

# =============================================================================
# CONFIGURATION & SETUP
# =============================================================================

# Environment optimization
os.environ["NUMEXPR_MAX_THREADS"] = "8"
os.environ["OMP_NUM_THREADS"] = "4"
os.environ["PYTHONWARNINGS"] = "ignore"
warnings.filterwarnings("ignore")

# Streamlit configuration
st.set_page_config(
    page_title="Institutional Commodities Platform v6.0",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://github.com/institutional-commodities',
        'Report a bug': "https://github.com/institutional-commodities/issues",
        'About': """🏛️ Institutional Commodities Analytics v6.0
                    Advanced analytics platform for institutional commodity trading
                    © 2024 Institutional Trading Analytics"""
    }
)

# Build identifier (helps verify the deployed code on Streamlit Cloud)
try:
    st.sidebar.caption(f"Build: {__ICD_BUILD__}")
except Exception:
    pass


# =============================================================================
# DATA STRUCTURES & CONFIGURATION
# =============================================================================

class AssetCategory(Enum):
    """Asset categories for classification"""
    PRECIOUS_METALS = "Precious Metals"
    INDUSTRIAL_METALS = "Industrial Metals"
    ENERGY = "Energy"
    AGRICULTURE = "Agriculture"
    BENCHMARK = "Benchmark"

@dataclass
class AssetMetadata:
    """Enhanced metadata for assets"""
    symbol: str
    name: str
    category: AssetCategory
    color: str
    description: str = ""
    exchange: str = "CME"
    contract_size: str = "Standard"
    margin_requirement: float = 0.05
    tick_size: float = 0.01
    enabled: bool = True
    risk_level: str = "Medium"  # Low, Medium, High
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

@dataclass
class AnalysisConfiguration:
    """Comprehensive analysis configuration"""
    start_date: datetime= field(default_factory=lambda: (datetime.now() - timedelta(days=1095)))
    end_date: datetime= field(default_factory=lambda: datetime.now())
    risk_free_rate: float = 0.02
    annual_trading_days: int = 252
    confidence_levels: Tuple[float, ...] = (0.90, 0.95, 0.99)
    garch_p_range: Tuple[int, int] = (1, 3)
    garch_q_range: Tuple[int, int] = (1, 3)
    regime_states: int = 3
    backtest_window: int = 250
    rolling_window: int = 60
    volatility_window: int = 20
    monte_carlo_simulations: int = 10000
    optimization_method: str = "sharpe"  # sharpe, min_var, max_ret
    
    def validate(self) -> bool:
        """Validate configuration parameters"""
        if self.start_date >= self.end_date:
            return False
        if not (0 <= self.risk_free_rate <= 1):
            return False
        if not all(0.5 <= cl <= 0.999 for cl in self.confidence_levels):
            return False
        return True

# Enhanced commodities universe with comprehensive metadata
COMMODITIES_UNIVERSE = {
    AssetCategory.PRECIOUS_METALS.value: {
        "GC=F": AssetMetadata(
            symbol="GC=F",
            name="Gold Futures",
            category=AssetCategory.PRECIOUS_METALS,
            color="#FFD700",
            description="COMEX Gold Futures (100 troy ounces)",
            exchange="COMEX",
            contract_size="100 troy oz",
            margin_requirement=0.045,
            tick_size=0.10,
            risk_level="Low"
        ),
        "SI=F": AssetMetadata(
            symbol="SI=F",
            name="Silver Futures",
            category=AssetCategory.PRECIOUS_METALS,
            color="#C0C0C0",
            description="COMEX Silver Futures (5,000 troy ounces)",
            exchange="COMEX",
            contract_size="5,000 troy oz",
            margin_requirement=0.065,
            tick_size=0.005,
            risk_level="Medium"
        ),
        "PL=F": AssetMetadata(
            symbol="PL=F",
            name="Platinum Futures",
            category=AssetCategory.PRECIOUS_METALS,
            color="#E5E4E2",
            description="NYMEX Platinum Futures (50 troy ounces)",
            exchange="NYMEX",
            contract_size="50 troy oz",
            margin_requirement=0.075,
            tick_size=0.10,
            risk_level="High"
        ),
    },
    AssetCategory.INDUSTRIAL_METALS.value: {
        "HG=F": AssetMetadata(
            symbol="HG=F",
            name="Copper Futures",
            category=AssetCategory.INDUSTRIAL_METALS,
            color="#B87333",
            description="COMEX Copper Futures (25,000 pounds)",
            exchange="COMEX",
            contract_size="25,000 lbs",
            margin_requirement=0.085,
            tick_size=0.0005,
            risk_level="Medium"
        ),
        "ALI=F": AssetMetadata(
            symbol="ALI=F",
            name="Aluminum Futures",
            category=AssetCategory.INDUSTRIAL_METALS,
            color="#848482",
            description="COMEX Aluminum Futures (44,000 pounds)",
            exchange="COMEX",
            contract_size="44,000 lbs",
            margin_requirement=0.095,
            tick_size=0.0001,
            risk_level="High"
        ),
    },
    AssetCategory.ENERGY.value: {
        "CL=F": AssetMetadata(
            symbol="CL=F",
            name="Crude Oil WTI",
            category=AssetCategory.ENERGY,
            color="#000000",
            description="NYMEX Light Sweet Crude Oil (1,000 barrels)",
            exchange="NYMEX",
            contract_size="1,000 barrels",
            margin_requirement=0.085,
            tick_size=0.01,
            risk_level="High"
        ),
        "NG=F": AssetMetadata(
            symbol="NG=F",
            name="Natural Gas",
            category=AssetCategory.ENERGY,
            color="#4169E1",
            description="NYMEX Natural Gas (10,000 MMBtu)",
            exchange="NYMEX",
            contract_size="10,000 MMBtu",
            margin_requirement=0.095,
            tick_size=0.001,
            risk_level="High"
        ),
    },
    AssetCategory.AGRICULTURE.value: {
        "ZC=F": AssetMetadata(
            symbol="ZC=F",
            name="Corn Futures",
            category=AssetCategory.AGRICULTURE,
            color="#FFD700",
            description="CBOT Corn Futures (5,000 bushels)",
            exchange="CBOT",
            contract_size="5,000 bushels",
            margin_requirement=0.065,
            tick_size=0.0025,
            risk_level="Medium"
        ),
        "ZW=F": AssetMetadata(
            symbol="ZW=F",
            name="Wheat Futures",
            category=AssetCategory.AGRICULTURE,
            color="#F5DEB3",
            description="CBOT Wheat Futures (5,000 bushels)",
            exchange="CBOT",
            contract_size="5,000 bushels",
            margin_requirement=0.075,
            tick_size=0.0025,
            risk_level="Medium"
        ),
    }
}

BENCHMARKS = {
    "^GSPC": {
        "name": "S&P 500 Index",
        "type": "equity",
        "color": "#1E90FF",
        "description": "S&P 500 Equity Index"
    },
    "DX-Y.NYB": {
        "name": "US Dollar Index",
        "type": "currency",
        "color": "#32CD32",
        "description": "US Dollar Currency Index"
    },
    "TLT": {
        "name": "20+ Year Treasury ETF",
        "type": "fixed_income",
        "color": "#8A2BE2",
        "description": "Long-term US Treasury Bonds"
    },
    "GLD": {
        "name": "SPDR Gold Shares",
        "type": "commodity",
        "color": "#FFD700",
        "description": "Gold-backed ETF"
    },
    "DBC": {
        "name": "Invesco DB Commodity Index",
        "type": "commodity",
        "color": "#FF6347",
        "description": "Broad Commodities ETF"
    }
}

# =============================================================================
# ADVANCED STYLES & THEMING
# =============================================================================

class ThemeManager:
    """Manage application theming and styling"""
    
    THEMES = {
        "default": {
            "primary": "#1a2980",
            "secondary": "#26d0ce",
            "accent": "#7c3aed",
            "success": "#10b981",
            "warning": "#f59e0b",
            "danger": "#ef4444",
            "dark": "#1f2937",
            "light": "#f3f4f6",
            "gray": "#6b7280",
            "background": "#ffffff"
        },
        "dark": {
            "primary": "#3b82f6",
            "secondary": "#06b6d4",
            "accent": "#8b5cf6",
            "success": "#10b981",
            "warning": "#f59e0b",
            "danger": "#ef4444",
            "dark": "#111827",
            "light": "#374151",
            "gray": "#9ca3af",
            "background": "#1f2937"
        }
    }
    
    @staticmethod
    def get_styles(theme: str = "default") -> str:
        """Get CSS styles for selected theme"""
        colors = ThemeManager.THEMES.get(theme, ThemeManager.THEMES["default"])
        
        return f"""
        <style>
            :root {{
                --primary: {colors['primary']};
                --secondary: {colors['secondary']};
                --accent: {colors['accent']};
                --success: {colors['success']};
                --warning: {colors['warning']};
                --danger: {colors['danger']};
                --dark: {colors['dark']};
                --light: {colors['light']};
                --gray: {colors['gray']};
                --background: {colors['background']};
                --shadow-sm: 0 1px 3px rgba(0,0,0,0.12), 0 1px 2px rgba(0,0,0,0.24);
                --shadow-md: 0 4px 6px rgba(0,0,0,0.1), 0 2px 4px rgba(0,0,0,0.06);
                --shadow-lg: 0 10px 25px rgba(0,0,0,0.15), 0 5px 10px rgba(0,0,0,0.05);
                --shadow-xl: 0 20px 40px rgba(0,0,0,0.2), 0 10px 20px rgba(0,0,0,0.1);
                --radius-sm: 6px;
                --radius-md: 10px;
                --radius-lg: 16px;
                --radius-xl: 24px;
                --transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            }}
            
            /* Main Header */
            .main-header {{
                background: linear-gradient(135deg, var(--primary) 0%, var(--secondary) 100%);
                padding: 2.5rem;
                border-radius: var(--radius-lg);
                color: white;
                margin-bottom: 2rem;
                box-shadow: var(--shadow-xl);
                position: relative;
                overflow: hidden;
                backdrop-filter: blur(10px);
                border: 1px solid rgba(255, 255, 255, 0.1);
            }}
            
            .main-header::before {{
                content: '';
                position: absolute;
                top: -50%;
                left: -50%;
                width: 200%;
                height: 200%;
                background: radial-gradient(circle, rgba(255,255,255,0.1) 1px, transparent 1px);
                background-size: 30px 30px;
                opacity: 0.4;
                animation: float 25s linear infinite;
            }}
            
            @keyframes float {{
                0% {{ transform: translate(0, 0) rotate(0deg); }}
                100% {{ transform: translate(-30px, -30px) rotate(360deg); }}
            }}
            
            /* Cards */
            .metric-card {{
                background: var(--background);
                padding: 1.75rem;
                border-radius: var(--radius-md);
                box-shadow: var(--shadow-md);
                border-left: 5px solid var(--primary);
                margin-bottom: 1.5rem;
                transition: var(--transition);
                border: 1px solid rgba(0,0,0,0.05);
            }}
            
            .metric-card:hover {{
                transform: translateY(-8px);
                box-shadow: var(--shadow-lg);
                border-color: var(--primary);
            }}
            
            .metric-card.glow {{
                animation: pulse-glow 2s infinite;
            }}
            
            @keyframes pulse-glow {{
                0%, 100% {{ box-shadow: 0 0 20px rgba(26, 41, 128, 0.2); }}
                50% {{ box-shadow: 0 0 40px rgba(26, 41, 128, 0.4); }}
            }}
            
            .metric-value {{
                font-size: 2.4rem;
                font-weight: 800;
                color: var(--dark);
                margin: 0.75rem 0;
                font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
                background: linear-gradient(135deg, var(--primary), var(--secondary));
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                background-clip: text;
            }}
            
            .metric-label {{
                font-size: 0.85rem;
                color: var(--gray);
                text-transform: uppercase;
                letter-spacing: 1.2px;
                font-weight: 600;
                display: flex;
                align-items: center;
                gap: 0.5rem;
            }}
            
            /* Badges */
            .status-badge {{
                display: inline-flex;
                align-items: center;
                gap: 0.5rem;
                padding: 0.5rem 1.25rem;
                border-radius: 50px;
                font-size: 0.85rem;
                font-weight: 700;
                text-transform: uppercase;
                transition: var(--transition);
                backdrop-filter: blur(10px);
                border: 1px solid rgba(255, 255, 255, 0.1);
            }}
            
            .status-success {{
                background: linear-gradient(135deg, var(--success) 0%, #059669 100%);
                color: white;
            }}
            
            .status-warning {{
                background: linear-gradient(135deg, var(--warning) 0%, #d97706 100%);
                color: white;
            }}
            
            .status-danger {{
                background: linear-gradient(135deg, var(--danger) 0%, #dc2626 100%);
                color: white;
            }}
            
            .status-info {{
                background: linear-gradient(135deg, #3b82f6 0%, #1d4ed8 100%);
                color: white;
            }}
            
            .status-badge:hover {{
                transform: scale(1.05);
                box-shadow: var(--shadow-md);
            }}
            
            /* Sidebar */
            .sidebar-section {{
                background: var(--light);
                padding: 1.75rem;
                border-radius: var(--radius-md);
                margin-bottom: 1.5rem;
                border-left: 4px solid var(--primary);
                transition: var(--transition);
                box-shadow: var(--shadow-sm);
            }}
            
            .sidebar-section:hover {{
                background: var(--background);
                box-shadow: var(--shadow-md);
                transform: translateX(5px);
            }}
            
            /* Tabs Enhancement */
            .stTabs [data-baseweb="tab-list"] {{
                gap: 12px;
                background-color: var(--light);
                padding: 12px;
                border-radius: var(--radius-lg);
                margin-bottom: 2rem;
            }}
            
            .stTabs [data-baseweb="tab"] {{
                border-radius: var(--radius-md);
                padding: 12px 24px;
                background-color: var(--background);
                border: 2px solid transparent;
                transition: var(--transition);
                font-weight: 600;
            }}
            
            .stTabs [aria-selected="true"] {{
                background: linear-gradient(135deg, var(--primary), var(--secondary));
                color: white;
                border-color: var(--primary);
                transform: translateY(-2px);
                box-shadow: var(--shadow-md);
            }}
            
            /* Dataframe Styling */
            .dataframe {{
                border-radius: var(--radius-md);
                overflow: hidden;
                border: 1px solid var(--light);
                box-shadow: var(--shadow-sm);
            }}
            
            .dataframe thead {{
                background: linear-gradient(135deg, var(--primary), var(--secondary));
                color: white;
            }}
            
            /* Loading Animations */
            @keyframes shimmer {{
                0% {{ background-position: -200px 0; }}
                100% {{ background-position: calc(200px + 100%) 0; }}
            }}
            
            .shimmer {{
                background: linear-gradient(90deg, var(--light) 0%, var(--background) 50%, var(--light) 100%);
                background-size: 200px 100%;
                animation: shimmer 1.5s infinite;
            }}
            
            /* Progress Bars */
            .stProgress > div > div > div {{
                background: linear-gradient(90deg, var(--primary), var(--secondary));
            }}
            
            /* Custom Scrollbar */
            ::-webkit-scrollbar {{
                width: 8px;
                height: 8px;
            }}
            
            ::-webkit-scrollbar-track {{
                background: var(--light);
                border-radius: 4px;
            }}
            
            ::-webkit-scrollbar-thumb {{
                background: linear-gradient(135deg, var(--primary), var(--secondary));
                border-radius: 4px;
            }}
            
            ::-webkit-scrollbar-thumb:hover {{
                background: linear-gradient(135deg, var(--secondary), var(--primary));
            }}
            
            /* Tooltips */
            .custom-tooltip {{
                position: relative;
                display: inline-block;
                cursor: help;
            }}
            
            .custom-tooltip:hover::after {{
                content: attr(data-tooltip);
                position: absolute;
                bottom: 125%;
                left: 50%;
                transform: translateX(-50%);
                background: var(--dark);
                color: white;
                padding: 0.75rem 1rem;
                border-radius: var(--radius-sm);
                font-size: 0.85rem;
                white-space: nowrap;
                z-index: 1000;
                box-shadow: var(--shadow-lg);
                backdrop-filter: blur(10px);
                border: 1px solid rgba(255, 255, 255, 0.1);
                opacity: 0;
                animation: fadeIn 0.3s forwards;
            }}
            
            @keyframes fadeIn {{
                to {{ opacity: 1; }}
            }}
            
            /* Section Headers */
            .section-header {{
                display: flex;
                align-items: center;
                gap: 1rem;
                margin: 2rem 0 1.5rem;
                padding-bottom: 0.75rem;
                border-bottom: 2px solid var(--primary);
            }}
            
            .section-header h2 {{
                margin: 0;
                color: var(--dark);
                font-size: 1.5rem;
                font-weight: 700;
            }}
            
            /* Grid Layout */
            .metric-grid {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                gap: 1.5rem;
                margin: 2rem 0;
            }}
            
            /* Responsive Design */
            @media (max-width: 768px) {{
                .metric-grid {{
                    grid-template-columns: 1fr;
                }}
                
                .main-header {{
                    padding: 1.5rem;
                }}
                
                .metric-value {{
                    font-size: 2rem;
                }}
            }}
        </style>
        """

# Apply default theme
st.markdown(ThemeManager.get_styles("default"), unsafe_allow_html=True)

# =============================================================================
# IMPORT MANAGEMENT & DEPENDENCY HANDLING
# =============================================================================

class DependencyManager:
    """Manage optional dependencies with graceful fallbacks"""
    
    def __init__(self):
        self.dependencies = {}
        self._load_dependencies()
    
    def _load_dependencies(self):
        """Load optional dependencies"""
        # statsmodels
        try:
            from statsmodels.stats.diagnostic import het_arch, acorr_ljungbox
            import statsmodels.api as sm
            from statsmodels.regression.rolling import RollingOLS
            self.dependencies['statsmodels'] = {
                'available': True,
                'module': sm,
                'het_arch': het_arch,
                'acorr_ljungbox': acorr_ljungbox,
                'RollingOLS': RollingOLS
            }
        except ImportError:
            self.dependencies['statsmodels'] = {'available': False}
            if st.session_state.get('show_system_diagnostics', False):
                st.warning("⚠️ statsmodels not available - some features disabled")
        # arch
        try:
            from arch import arch_model
            self.dependencies['arch'] = {
                'available': True,
                'arch_model': arch_model
            }
        except ImportError:
            self.dependencies['arch'] = {'available': False}
            if st.session_state.get('show_system_diagnostics', False):
                st.warning("⚠️ arch not available - GARCH features disabled")
        # hmmlearn & sklearn
        try:
            from hmmlearn.hmm import GaussianHMM
            from sklearn.preprocessing import StandardScaler
            from sklearn.cluster import KMeans
            from sklearn.decomposition import PCA
            self.dependencies['hmmlearn'] = {
                'available': True,
                'GaussianHMM': GaussianHMM,
                'StandardScaler': StandardScaler,
                'KMeans': KMeans,
                'PCA': PCA
            }
        except ImportError:
            self.dependencies['hmmlearn'] = {'available': False}
            st.info("ℹ️ hmmlearn/scikit-learn not available - regime detection disabled")
        
        # quantstats
        try:
            import quantstats as qs
            self.dependencies['quantstats'] = {
                'available': True,
                'module': qs
            }
        except ImportError:
            self.dependencies['quantstats'] = {'available': False}
        
        # ta (technical analysis)
        try:
            import ta
            self.dependencies['ta'] = {
                'available': True,
                'module': ta
            }
        except ImportError:
            self.dependencies['ta'] = {'available': False}
    
    def is_available(self, dependency: str) -> bool:
        """Check if dependency is available"""
        return self.dependencies.get(dependency, {}).get('available', False)
    
    def get_module(self, dependency: str):
        """Get dependency module if available"""
        dep = self.dependencies.get(dependency, {})
        return dep.get('module') if dep.get('available') else None

# Initialize dependency manager
dep_manager = DependencyManager()

# =============================================================================
# ADVANCED CACHING SYSTEM
# =============================================================================

class SmartCache:
    """Advanced caching with memory management, TTL, and persistence"""
    
    def __init__(self, max_entries: int = 100, ttl_hours: int = 24):
        self.max_entries = max_entries
        self.ttl_seconds = ttl_hours * 3600
    
    @staticmethod
    def generate_key(*args, **kwargs) -> str:
        """Generate cache key from arguments"""
        key_parts = []
        
        # Add positional arguments
        for arg in args:
            if isinstance(arg, (str, int, float, bool, type(None))):
                key_parts.append(str(arg))
            elif isinstance(arg, (datetime, pd.Timestamp)):
                key_parts.append(arg.isoformat())
            elif isinstance(arg, pd.DataFrame):
                # Create hash from DataFrame content
                content_hash = hashlib.md5(
                    pd.util.hash_pandas_object(arg).values.tobytes()
                ).hexdigest()
                key_parts.append(content_hash)
            else:
                key_parts.append(str(hash(str(arg))))
        
        # Add keyword arguments
        for k, v in sorted(kwargs.items()):
            key_parts.append(f"{k}:{v}")
        
        return hashlib.md5("_".join(key_parts).encode()).hexdigest()
    
    @staticmethod
    def cache_data(ttl: int = 3600, max_entries: int = 50):
        """Decorator for caching data with TTL"""
        def decorator(func):
            @wraps(func)
            @st.cache_data(ttl=ttl, max_entries=max_entries, show_spinner=False)
            def wrapper(_arg0, *args, **kwargs):
                try:
                    return func(_arg0, *args, **kwargs)
                except Exception as e:
                    st.warning(f"Cache miss for {func.__name__}: {str(e)[:100]}")
                    # Clear cache for this function on error
                    st.cache_data.clear()
                    return func(_arg0, *args, **kwargs)
            return wrapper
        return decorator
    
    @staticmethod
    def cache_resource(max_entries: int = 20):
        """Decorator for caching resources"""
        def decorator(func):
            @wraps(func)
            @st.cache_resource(max_entries=max_entries)
            def wrapper(_arg0, *args, **kwargs):
                return func(_arg0, *args, **kwargs)
            return wrapper
        return decorator

# =============================================================================
# ENHANCED DATA MANAGER
# =============================================================================

class EnhancedDataManager:
    """Advanced data management with intelligent fetching and preprocessing"""
    
    def __init__(self):
        self.cache = SmartCache()
    
    @SmartCache.cache_data(ttl=7200, max_entries=100)
    def fetch_asset_data(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        interval: str = "1d",
        retries: int = 3
    ) -> pd.DataFrame:
        """Fetch and preprocess asset data with intelligent retry logic"""
        cache_key = self.cache.generate_key(
            "fetch_asset", symbol, start_date, end_date, interval
        )
        
        for attempt in range(retries):
            try:
                # Configure yfinance download
                download_params = {
                    'tickers': symbol,
                    'start': start_date,
                    'end': end_date,
                    'interval': interval,
                    'progress': False,
                    'auto_adjust': True,
                    'threads': True,
                    'timeout': 30
                }
                
                # Try different download strategies
                if attempt == 0:
                    # First attempt: standard download
                    df = yf_download_safe(download_params)
                elif attempt == 1:
                    # Second attempt: force direct download
                    download_params['auto_adjust'] = False
                    df = yf_download_safe(download_params)
                else:
                    # Third attempt: try with different parameters
                    download_params['interval'] = "1d"
                    download_params['period'] = "max"
                    df = yf_download_safe(download_params)
                    # Filter by date
                    df = df[df.index >= pd.Timestamp(start_date)]
                    df = df[df.index <= pd.Timestamp(end_date)]
                
                if not isinstance(df, pd.DataFrame) or df.empty:
                    raise ValueError(f"No data returned for {symbol}")
                
                # Clean and validate data
                df = self._clean_dataframe(df, symbol)
                
                if len(df) < 20:  # Minimum data points
                    raise ValueError(f"Insufficient data for {symbol}")
                
                return df
                
            except Exception as e:
                if attempt == retries - 1:
                    st.warning(f"Failed to fetch {symbol} after {retries} attempts: {str(e)[:150]}")
                    return pd.DataFrame()
                continue
        
        return pd.DataFrame()
    
    def _clean_dataframe(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """Clean and validate dataframe"""
        df = df.copy()
        
        # Handle MultiIndex columns
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [col[0] for col in df.columns]
        
        # Clean column names
        df.columns = [str(col).strip().replace(' ', '_') for col in df.columns]
        
        # Ensure required columns exist
        required_cols = ['Close', 'Open', 'High', 'Low', 'Volume']
        
        # Map columns
        col_mapping = {}
        for col in required_cols:
            if col not in df.columns:
                # Try to find similar columns
                for actual_col in df.columns:
                    if col.lower() in actual_col.lower():
                        col_mapping[col] = actual_col
                        break
        
        # Create missing columns
        if 'Adj_Close' not in df.columns and 'Close' in df.columns:
            df['Adj_Close'] = df['Close']
        
        if 'Close' not in df.columns:
            if 'Adj_Close' in df.columns:
                df['Close'] = df['Adj_Close']
            elif len(df.columns) > 0:
                df['Close'] = df.iloc[:, -1]
            else:
                return pd.DataFrame()
        
        # Fill missing OHLC data
        for col in ['Open', 'High', 'Low']:
            if col not in df.columns:
                df[col] = df['Close']
        
        # Ensure Adj_Close exists (yfinance auto_adjust may remove it)
        
        if 'Adj_Close' not in df.columns:
        
            df['Adj_Close'] = df['Close']

        
        if 'Volume' not in df.columns:
            df['Volume'] = 0.0
        
        # Clean index
        df.index = pd.to_datetime(df.index)
        df = df[~df.index.duplicated(keep='last')]
        df = df.sort_index()
        
        # Remove rows with NaN in critical columns
        critical_cols = ['Close', 'Adj_Close']
        df = df.dropna(subset=[col for col in critical_cols if col in df.columns])
        
        return df
    
    @SmartCache.cache_data(ttl=3600, max_entries=50)
    def fetch_multiple_assets(
        self,
        symbols: List[str],
        start_date: datetime,
        end_date: datetime,
        max_workers: int = 4
    ) -> Dict[str, pd.DataFrame]:
        """Parallel fetch of multiple assets"""
        results = {}
        failed_symbols = []
        
        with ThreadPoolExecutor(max_workers=min(max_workers, len(symbols))) as executor:
            # Create futures
            future_to_symbol = {}
            for symbol in symbols:
                future = executor.submit(
                    self.fetch_asset_data,
                    symbol,
                    start_date,
                    end_date
                )
                future_to_symbol[future] = symbol
            
            # Process results as they complete
            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    df = future.result()
                    if not df.empty:
                        results[symbol] = df
                    else:
                        failed_symbols.append(symbol)
                except Exception as e:
                    failed_symbols.append(symbol)
                    continue
        
        # Log failures
        if failed_symbols:
            st.info(f"Failed to load {len(failed_symbols)} symbols: {', '.join(failed_symbols[:5])}")
        
        return results
    
    def calculate_technical_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate comprehensive technical features"""
        df = df.copy()
        
        # Ensure Adj Close exists
        if 'Adj_Close' not in df.columns and 'Close' in df.columns:
            df['Adj_Close'] = df['Close']
        
        price_col = 'Adj_Close' if 'Adj_Close' in df.columns else 'Close'
        
        # Returns
        df['Returns'] = df[price_col].pct_change()
        df['Log_Returns'] = np.log(df[price_col] / df[price_col].shift(1))
        
        # Price statistics
        df['Price_Range'] = (df['High'] - df['Low']) / df[price_col]
        df['Price_Change'] = df[price_col].diff()
        
        # Moving averages
        periods = [5, 10, 20, 50, 100, 200]
        for period in periods:
            df[f'SMA_{period}'] = df[price_col].rolling(window=period).mean()
            df[f'EMA_{period}'] = df[price_col].ewm(span=period).mean()
        
        # Bollinger Bands
        bb_period = 20
        bb_middle = df[price_col].rolling(window=bb_period).mean()
        bb_std = df[price_col].rolling(window=bb_period).std()
        df['BB_Upper'] = bb_middle + (bb_std * 2)
        df['BB_Lower'] = bb_middle - (bb_std * 2)
        df['BB_Width'] = (df['BB_Upper'] - df['BB_Lower']) / bb_middle
        df['BB_Position'] = (df[price_col] - df['BB_Lower']) / (df['BB_Upper'] - df['BB_Lower'])
        
        # RSI
        delta = df[price_col].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['RSI'] = 100 - (100 / (1 + rs))
        
        # MACD
        ema12 = df[price_col].ewm(span=12).mean()
        ema26 = df[price_col].ewm(span=26).mean()
        df['MACD'] = ema12 - ema26
        df['MACD_Signal'] = df['MACD'].ewm(span=9).mean()
        df['MACD_Histogram'] = df['MACD'] - df['MACD_Signal']
        
        # Volatility measures
        df['Volatility_20D'] = df['Returns'].rolling(window=20).std() * np.sqrt(252)
        df['Volatility_60D'] = df['Returns'].rolling(window=60).std() * np.sqrt(252)
        df['Realized_Vol'] = df['Returns'].rolling(window=20).std() * np.sqrt(252)
        
        # Volume indicators
        if 'Volume' in df.columns:
            df['Volume_SMA_20'] = df['Volume'].rolling(window=20).mean()
            df['Volume_Ratio'] = df['Volume'] / df['Volume_SMA_20']
            df['Volume_Adjusted'] = df['Volume'] * df[price_col]
        
        # ATR (Average True Range)
        high_low = df['High'] - df['Low']
        high_close = np.abs(df['High'] - df[price_col].shift())
        low_close = np.abs(df['Low'] - df[price_col].shift())
        true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        df['ATR'] = true_range.rolling(window=14).mean()
        df['ATR_Pct'] = df['ATR'] / df[price_col] * 100
        
        # Momentum indicators
        df['Momentum_10D'] = df[price_col].pct_change(periods=10)
        df['Momentum_20D'] = df[price_col].pct_change(periods=20)
        
        # Rate of Change
        df['ROC_10'] = ((df[price_col] - df[price_col].shift(10)) / df[price_col].shift(10)) * 100
        df['ROC_20'] = ((df[price_col] - df[price_col].shift(20)) / df[price_col].shift(20)) * 100
        
        # Williams %R
        period = 14
        highest_high = df['High'].rolling(window=period).max()
        lowest_low = df['Low'].rolling(window=period).min()
        df['Williams_%R'] = ((highest_high - df[price_col]) / (highest_high - lowest_low)) * -100
        
        # Stochastic Oscillator
        df['Stochastic_%K'] = ((df[price_col] - lowest_low) / (highest_high - lowest_low)) * 100
        df['Stochastic_%D'] = df['Stochastic_%K'].rolling(window=3).mean()
        
        # Commodity Channel Index (CCI)
        typical_price = (df['High'] + df['Low'] + df[price_col]) / 3
        cci_sma = typical_price.rolling(window=20).mean()
        cci_mean_dev = typical_price.rolling(window=20).apply(
            lambda x: np.mean(np.abs(x - x.mean()))
        )
        df['CCI'] = (typical_price - cci_sma) / (0.015 * cci_mean_dev)
        
        # On Balance Volume
        if 'Volume' in df.columns:
            df['OBV'] = (np.sign(df['Returns'].fillna(0)) * df['Volume']).cumsum()
        
        # Price trends
        df['Trend_Strength'] = df['Returns'].rolling(window=20).apply(
            lambda x: np.corrcoef(np.arange(len(x)), x)[0, 1] if len(x) > 1 else 0
        )
        
        # Drop NaN values from feature calculations
        df = df.dropna(subset=['Returns', 'Volatility_20D'])
        
        return df

# =============================================================================
# ADVANCED ANALYTICS ENGINE
# =============================================================================

class InstitutionalAnalytics:
    """Institutional-grade analytics engine with advanced methods"""
    
    def __init__(self, risk_free_rate: float = 0.02):
        self.risk_free_rate = risk_free_rate
        self.annual_trading_days = 252


    # =========================================================================
    # NUMERICAL STABILITY HELPERS (Higham-style PSD / correlation repairs)
    # =========================================================================

    @staticmethod
    def _symmetrize(a: np.ndarray) -> np.ndarray:
        """Force symmetry (numerical hygiene)."""
        a = np.asarray(a, dtype=float)
        return 0.5 * (a + a.T)

    @staticmethod
    def _project_psd(a: np.ndarray, epsilon: float = 1e-12) -> np.ndarray:
        """Projection onto the PSD cone via eigenvalue clipping."""
        a = InstitutionalAnalytics._symmetrize(a)
        vals, vecs = np.linalg.eigh(a)
        vals = np.clip(vals, epsilon, None)
        return InstitutionalAnalytics._symmetrize((vecs * vals) @ vecs.T)

    def _higham_nearest_correlation(
        self,
        corr: np.ndarray,
        max_iter: int = 100,
        tol: float = 1e-7,
        epsilon: float = 1e-12,
    ) -> np.ndarray:
        """Higham (2002)-style alternating projections to the nearest correlation matrix.

        This is a defensive routine to prevent hard crashes in downstream routines
        (optimization / Cholesky) when a correlation estimate becomes indefinite
        due to missing data, rounding, or numerical noise.
        """
        a = self._symmetrize(np.asarray(corr, dtype=float))
        # Ensure diagonal starts at 1
        np.fill_diagonal(a, 1.0)

        y = a.copy()
        delta_s = np.zeros_like(y)

        # Frobenius norm scale (avoid divide by 0)
        base = np.linalg.norm(a, ord="fro")
        if not np.isfinite(base) or base <= 0:
            base = 1.0

        for _ in range(int(max_iter)):
            r = y - delta_s
            x = self._project_psd(r, epsilon=epsilon)
            delta_s = x - r

            y = x.copy()
            np.fill_diagonal(y, 1.0)
            y = self._symmetrize(y)

            rel = np.linalg.norm(y - x, ord="fro") / base
            if rel < float(tol):
                break

        # Final PSD polish (rare edge cases)
        y = self._project_psd(y, epsilon=epsilon)
        np.fill_diagonal(y, 1.0)
        return self._symmetrize(y)

    def _ensure_psd_covariance(
        self,
        cov: pd.DataFrame,
        method: str = "higham",
        epsilon: float = 1e-12,
        max_iter: int = 100,
        tol: float = 1e-7,
    ) -> pd.DataFrame:
        """Return a symmetric PSD covariance matrix (defensive; preserves variances).

        Parameters
        ----------
        cov : pd.DataFrame
            Sample covariance estimate (may be indefinite with missing data / noise).
        method : str
            'higham' (default): convert to correlation, apply Higham, convert back.
            'eigen_clip': direct eigenvalue clipping on covariance (fast, less strict).
        """
        if cov is None or cov.empty:
            return cov

        cov_work = cov.copy().astype(float)
        cov_work = cov_work.fillna(0.0)
        cov_work.values[:] = self._symmetrize(cov_work.values)

        # Defensive variance floor
        diag = np.diag(cov_work.values).copy()
        diag = np.where(np.isfinite(diag), diag, 0.0)
        diag = np.maximum(diag, float(epsilon))

        if str(method).lower().strip() == "eigen_clip":
            repaired = self._project_psd(cov_work.values, epsilon=float(epsilon))
            # Keep original variances (important for interpretation)
            np.fill_diagonal(repaired, diag)
            repaired = self._project_psd(repaired, epsilon=float(epsilon))
            repaired_df = pd.DataFrame(repaired, index=cov_work.index, columns=cov_work.columns)
            return repaired_df

        # Higham path: covariance -> correlation -> nearest correlation -> covariance
        d = np.sqrt(diag)
        d = np.where(d > 0, d, np.sqrt(float(epsilon)))
        inv_d = 1.0 / d
        corr = cov_work.values * inv_d[:, None] * inv_d[None, :]
        corr = self._symmetrize(corr)
        np.fill_diagonal(corr, 1.0)

        corr_psd = self._higham_nearest_correlation(
            corr,
            max_iter=int(max_iter),
            tol=float(tol),
            epsilon=float(epsilon),
        )

        cov_psd = corr_psd * d[:, None] * d[None, :]
        cov_psd = self._symmetrize(cov_psd)
        # Ensure variances preserved (numerical)
        np.fill_diagonal(cov_psd, diag)
        cov_psd = self._project_psd(cov_psd, epsilon=float(epsilon))
        np.fill_diagonal(cov_psd, diag)
        cov_psd = self._symmetrize(cov_psd)

        return pd.DataFrame(cov_psd, index=cov_work.index, columns=cov_work.columns)

    
    # =========================================================================
    # PERFORMANCE METRICS
    # =========================================================================
    
    def calculate_performance_metrics(
        self,
        returns: pd.Series,
        benchmark_returns: Optional[pd.Series] = None
    ) -> Dict[str, Any]:
        """Calculate comprehensive performance metrics"""
        returns = returns.dropna()
        
        if len(returns) < 20:
            return {}
        
        # Basic calculations
        cumulative = (1 + returns).cumprod()
        total_return = cumulative.iloc[-1] - 1
        
        # Annualized metrics
        years = len(returns) / self.annual_trading_days
        annual_return = (1 + total_return) ** (1 / years) - 1 if years > 0 else 0
        
        # Volatility and risk-adjusted returns
        annual_vol = returns.std() * np.sqrt(self.annual_trading_days)
        sharpe = (annual_return - self.risk_free_rate) / annual_vol if annual_vol > 0 else 0
        
        # Downside risk metrics
        downside_returns = returns[returns < 0]
        downside_vol = downside_returns.std() * np.sqrt(self.annual_trading_days) if len(downside_returns) > 1 else 0
        sortino = (annual_return - self.risk_free_rate) / downside_vol if downside_vol > 0 else 0
        
        # Drawdown analysis
        running_max = cumulative.cummax()
        drawdown = (cumulative - running_max) / running_max
        max_dd = drawdown.min()
        max_dd_duration = self._calculate_max_dd_duration(drawdown)
        
        # Calmar ratio
        calmar = annual_return / abs(max_dd) if max_dd != 0 else 0
        
        # Higher moments
        skewness = returns.skew()
        kurtosis = returns.kurtosis()
        
        # VaR and CVaR (95% and 99%)
        var_95 = np.percentile(returns, 5)
        var_99 = np.percentile(returns, 1)
        cvar_95 = returns[returns <= var_95].mean()
        cvar_99 = returns[returns <= var_99].mean()
        
        # Gain/Loss metrics
        positive_returns = returns[returns > 0]
        negative_returns = returns[returns < 0]
        
        win_rate = len(positive_returns) / len(returns) if len(returns) > 0 else 0
        avg_gain = positive_returns.mean() if len(positive_returns) > 0 else 0
        avg_loss = negative_returns.mean() if len(negative_returns) > 0 else 0
        profit_factor = abs(positive_returns.sum() / negative_returns.sum()) if negative_returns.sum() < 0 else float('inf')
        
        # Beta and Alpha (if benchmark provided)
        alpha = beta = treynor = information_ratio = tracking_error = 0
        
        if benchmark_returns is not None and len(benchmark_returns) > 0:
            # Align returns
            aligned = pd.concat([returns, benchmark_returns], axis=1, join='inner').dropna()
            if len(aligned) > 20:
                asset_ret = aligned.iloc[:, 0]
                bench_ret = aligned.iloc[:, 1]
                
                # Beta calculation
                cov_matrix = np.cov(asset_ret, bench_ret)
                beta = cov_matrix[0, 1] / cov_matrix[1, 1] if cov_matrix[1, 1] > 0 else 0
                
                # Alpha calculation
                alpha = annual_return - (self.risk_free_rate + beta * (bench_ret.mean() * self.annual_trading_days - self.risk_free_rate))
                
                # Treynor ratio
                treynor = (annual_return - self.risk_free_rate) / beta if beta != 0 else 0
                
                # Information ratio
                tracking_error = (asset_ret - bench_ret).std() * np.sqrt(self.annual_trading_days)
                information_ratio = (annual_return - bench_ret.mean() * self.annual_trading_days) / tracking_error if tracking_error > 0 else 0
        
        return {
            'total_return': total_return * 100,
            'annual_return': annual_return * 100,
            'annual_volatility': annual_vol * 100,
            'sharpe_ratio': sharpe,
            'sortino_ratio': sortino,
            'calmar_ratio': calmar,
            'max_drawdown': max_dd * 100,
            'max_dd_duration': max_dd_duration,
            'skewness': skewness,
            'kurtosis': kurtosis,
            'var_95': var_95 * 100,
            'var_99': var_99 * 100,
            'cvar_95': cvar_95 * 100,
            'cvar_99': cvar_99 * 100,
            'win_rate': win_rate * 100,
            'avg_gain': avg_gain * 100,
            'avg_loss': avg_loss * 100,
            'profit_factor': profit_factor if profit_factor != float('inf') else 1000,
            'alpha': alpha * 100,
            'beta': beta,
            'treynor_ratio': treynor,
            'information_ratio': information_ratio,
            'tracking_error': tracking_error * 100,
            'positive_returns': len(positive_returns),
            'negative_returns': len(negative_returns),
            'total_trades': len(returns),
            'years_data': years
        }
    
    def _calculate_max_dd_duration(self, drawdown: pd.Series) -> int:
        """Calculate maximum drawdown duration in days"""
        if drawdown.empty:
            return 0
        
        current_duration = 0
        max_duration = 0
        
        for dd in drawdown:
            if dd < 0:
                current_duration += 1
                max_duration = max(max_duration, current_duration)
            else:
                current_duration = 0
        
        return max_duration
    
    # =========================================================================
    # =========================================================================
    # EWMA VOLATILITY RATIO SIGNAL
    # =========================================================================

    def compute_ewma_volatility(
        self,
        returns: pd.Series,
        span: int = 22,
        annualize: bool = False
    ) -> pd.Series:
        """Compute EWMA volatility (std) from returns.

        Uses exponentially-weighted moving average of squared returns with adjust=False.
        Returns a volatility series (same index as input).
        """
        try:
            r = pd.to_numeric(returns, errors="coerce").dropna()
            if r.empty or int(span) <= 1:
                return pd.Series(dtype=float)

            # EWMA variance
            var = (r ** 2).ewm(span=int(span), adjust=False, min_periods=max(5, int(span)//3)).mean()
            vol = np.sqrt(var)
            if annualize:
                vol = vol * np.sqrt(float(self.annual_trading_days))
            vol.name = f"EWMA_VOL_{int(span)}"
            return vol
        except Exception:
            return pd.Series(dtype=float)

    def compute_ewma_volatility_ratio(
        self,
        returns: pd.Series,
        span_fast: int = 22,
        span_mid: int = 33,
        span_slow: int = 99,
        annualize: bool = False
    ) -> pd.DataFrame:
        """Compute the institutional EWMA volatility ratio signal.

        Ratio definition (as requested):
            RATIO = EWMA_VOL(span_fast) / (EWMA_VOL(span_mid) + EWMA_VOL(span_slow))

        Returns a DataFrame with EWMA vols + ratio for charting/reporting.
        """
        try:
            r = pd.to_numeric(returns, errors="coerce").dropna()
            if r.empty:
                return pd.DataFrame()

            v_fast = self.compute_ewma_volatility(r, span=int(span_fast), annualize=annualize)
            v_mid  = self.compute_ewma_volatility(r, span=int(span_mid), annualize=annualize)
            v_slow = self.compute_ewma_volatility(r, span=int(span_slow), annualize=annualize)

            # Align
            df = pd.concat([v_fast, v_mid, v_slow], axis=1).dropna(how="any")
            if df.empty:
                return pd.DataFrame()

            denom = (df[v_mid.name] + df[v_slow.name]).replace(0.0, np.nan)
            ratio = (df[v_fast.name] / denom).rename("EWMA_RATIO")
            out = df.copy()
            out["EWMA_RATIO"] = ratio
            out = out.dropna(how="any")
            return out
        except Exception:
            return pd.DataFrame()

    # PORTFOLIO OPTIMIZATION
    # =========================================================================
    
    def optimize_portfolio(
        self,
        returns_df: pd.DataFrame,
        method: str = 'sharpe',
        constraints: Optional[Dict] = None,
        target_return: Optional[float] = None
    ) -> Dict[str, Any]:
        """Advanced portfolio optimization"""
        
        if returns_df.empty or len(returns_df) < 60:
            return {'success': False, 'message': 'Insufficient data'}
        
        n_assets = returns_df.shape[1]
        
        # Default constraints
        if constraints is None:
            constraints = {
                'min_weight': 0.0,
                'max_weight': 1.0,
                'sum_to_one': True
            }
        
        bounds = tuple((constraints['min_weight'], constraints['max_weight']) 
                      for _ in range(n_assets))
        
        # Initial weights
        init_weights = np.ones(n_assets) / n_assets
        
        # Define optimization constraints
        opt_constraints = []
        
        if constraints.get('sum_to_one', True):
            opt_constraints.append({'type': 'eq', 'fun': lambda w: np.sum(w) - 1})
        
        if target_return is not None:
            opt_constraints.append({
                'type': 'eq',
                'fun': lambda w: np.sum(returns_df.mean() * w) * self.annual_trading_days - target_return
            })
        # Define objective functions
        cov_matrix = returns_df.cov() * self.annual_trading_days
        mean_returns = returns_df.mean() * self.annual_trading_days

        # Defensive covariance repair (prevents hard crashes in sqrt / optimizer due to indefiniteness)
        try:
            cov_matrix = self._ensure_psd_covariance(
                cov_matrix,
                method="higham",
                epsilon=1e-12,
                max_iter=100,
                tol=1e-7,
            )
        except Exception as _psd_e:
            # Fallback to eigen-clip (very fast)
            try:
                cov_matrix = self._ensure_psd_covariance(
                    cov_matrix,
                    method="eigen_clip",
                    epsilon=1e-12,
                    max_iter=50,
                    tol=1e-6,
                )
            except Exception:
                # Last resort: numeric hygiene only
                cov_matrix = cov_matrix.fillna(0.0)
                cov_matrix = 0.5 * (cov_matrix + cov_matrix.T)
        def portfolio_variance(weights):
            return weights.T @ cov_matrix @ weights
        
        def portfolio_sharpe(weights):
            port_return = np.sum(mean_returns * weights)
            port_vol = np.sqrt(weights.T @ cov_matrix @ weights)
            return -(port_return - self.risk_free_rate) / port_vol if port_vol > 0 else 1e6
        
        def portfolio_return(weights):
            return -np.sum(mean_returns * weights)
        
        # Select objective function
        if method == 'sharpe':
            objective = portfolio_sharpe
        elif method == 'min_variance':
            objective = portfolio_variance
        elif method == 'max_return':
            objective = portfolio_return
        else:
            objective = portfolio_sharpe
        
        # Perform optimization
        try:
            result = optimize.minimize(
                objective,
                x0=init_weights,
                bounds=bounds,
                constraints=opt_constraints,
                method='SLSQP',
                options={'maxiter': 1000, 'ftol': 1e-9}
            )
            
            if result.success:
                optimized_weights = result.x
                optimized_weights = optimized_weights / np.sum(optimized_weights)  # Ensure sum to 1
                
                # Calculate portfolio metrics
                portfolio_returns = returns_df @ optimized_weights
                metrics = self.calculate_performance_metrics(portfolio_returns)
                
                # Calculate risk contributions
                risk_contributions = self._calculate_risk_contributions(
                    returns_df, optimized_weights
                )
                
                # Calculate diversification ratio
                diversification_ratio = self._calculate_diversification_ratio(
                    returns_df, optimized_weights
                )
                
                return {
                    'success': True,
                    'weights': dict(zip(returns_df.columns, optimized_weights)),
                    'metrics': metrics,
                    'risk_contributions': risk_contributions,
                    'diversification_ratio': diversification_ratio,
                    'objective_value': -result.fun if method == 'sharpe' else result.fun,
                    'n_iterations': result.nit
                }
            else:
                return {'success': False, 'message': result.message}
                
        except Exception as e:
            return {'success': False, 'message': str(e)}
    
    def _calculate_risk_contributions(
        self,
        returns_df: pd.DataFrame,
        weights: np.ndarray
    ) -> Dict[str, float]:
        """Calculate risk contributions for each asset"""
        cov_matrix = returns_df.cov() * self.annual_trading_days
        portfolio_variance = weights.T @ cov_matrix @ weights
        
        if portfolio_variance <= 0:
            return {asset: 0 for asset in returns_df.columns}
        
        marginal_contributions = (cov_matrix @ weights) / portfolio_variance
        risk_contributions = marginal_contributions * weights
        
        return dict(zip(returns_df.columns, risk_contributions * 100))
    
    def _calculate_diversification_ratio(
        self,
        returns_df: pd.DataFrame,
        weights: np.ndarray
    ) -> float:
        """Calculate diversification ratio"""
        asset_vols = returns_df.std() * np.sqrt(self.annual_trading_days)
        weighted_vol = np.sum(weights * asset_vols)
        portfolio_vol = np.sqrt(weights.T @ (returns_df.cov() * self.annual_trading_days) @ weights)
        
        return weighted_vol / portfolio_vol if portfolio_vol > 0 else 1.0
    
    # =========================================================================
    # GARCH MODELING
    # =========================================================================
    
    def garch_analysis(
        self,
        returns: pd.Series,
        p: Optional[int] = None,
        q: Optional[int] = None,
        p_range: Tuple[int, int] = (1, 2),
        q_range: Tuple[int, int] = (1, 2),
        distributions: List[str] = None,
        dist: Optional[str] = None,
        annualize: bool = True
    ) -> Dict[str, Any]:
        """Perform GARCH analysis with Cloud-safe behavior and UI-compatible output.

        Fixes:
        - Streamlit UI calls this method with `p=` and `q=`; we now accept those keywords.
        - Returns `success` (alias of `available`) and exposes `conditional_volatility` for plotting.
        - Annualizes conditional volatility to match the visualization (Realized Vol is annualized).
        """
        # Dependency gate
        if not dep_manager.is_available("arch"):
            return {
                "available": False,
                "success": False,
                "message": "ARCH package not available. Add `arch` to requirements.txt to enable GARCH.",
            }

        if distributions is None:
            distributions = ["normal", "t", "skewt"]
        if dist is not None:
            distributions = [str(dist)]

        # Robust return cleaning
        try:
            r = pd.to_numeric(returns, errors="coerce")
        except Exception:
            r = returns.copy()
        r = r.replace([np.inf, -np.inf], np.nan).dropna()
        try:
            r = r[~r.index.duplicated(keep="last")].sort_index()
        except Exception:
            pass

        if r is None or r.empty or len(r) < 60:
            return {
                "available": False,
                "success": False,
                "message": "Insufficient data for GARCH (need at least ~60 observations).",
                "n_obs": 0 if r is None else int(len(r)),
            }

        # Allow UI-style single model selection
        if p is not None:
            p_range = (int(p), int(p))
        if q is not None:
            q_range = (int(q), int(q))

        # Scale to percent for arch_model stability (common convention)
        returns_scaled = r.values.astype(float) * 100.0

        arch_model = dep_manager.dependencies["arch"]["arch_model"]

        annual_days = float(getattr(self.cfg, "annual_trading_days", 252))
        ann_scale = math.sqrt(annual_days) if annualize else 1.0

        results: List[Dict[str, Any]] = []
        best = None  # track best by BIC
        best_bic = None

        for pp in range(int(p_range[0]), int(p_range[1]) + 1):
            for qq in range(int(q_range[0]), int(q_range[1]) + 1):
                for d in distributions:
                    try:
                        model = arch_model(
                            returns_scaled,
                            mean="Constant",
                            vol="GARCH",
                            p=int(pp),
                            q=int(qq),
                            dist=str(d),
                            rescale=False
                        )
                        fit = model.fit(disp="off", show_warning=False, update_freq=0)

                        # Conditional vol from arch is in percent (because input is percent).
                        # Convert to annualized decimal to match plotting.
                        cond_vol = np.asarray(fit.conditional_volatility, dtype=float)  # percent (daily)
                        cond_vol_dec = (cond_vol / 100.0) * ann_scale  # annualized decimal (if annualize)

                        cond_series = pd.Series(cond_vol_dec, index=r.index[:len(cond_vol_dec)])

                        row = {
                            "p": int(pp),
                            "q": int(qq),
                            "distribution": str(d),
                            "aic": float(getattr(fit, "aic", np.nan)),
                            "bic": float(getattr(fit, "bic", np.nan)),
                            "log_likelihood": float(getattr(fit, "loglikelihood", np.nan)),
                            "converged": bool(getattr(fit, "convergence_flag", 1) == 0),
                            "params": dict(getattr(fit, "params", {})),
                            "conditional_volatility": cond_series,
                        }
                        results.append(row)

                        bic_val = row["bic"]
                        if np.isfinite(bic_val):
                            if best_bic is None or bic_val < best_bic:
                                best_bic = bic_val
                                best = row

                    except Exception:
                        continue

        if not results or best is None:
            return {
                "available": False,
                "success": False,
                "message": "No GARCH models converged.",
                "n_models_tested": int(len(results)),
            }

        # Prepare a lightweight best_model dict for JSON (exclude the heavy series)
        best_model_json = {k: v for k, v in best.items() if k != "conditional_volatility"}

        return {
            "available": True,
            "success": True,
            "message": "GARCH model fit successful.",
            "best_model": best_model_json,
            "all_models": [
                {k: v for k, v in row.items() if k != "conditional_volatility"} for row in results
            ],
            "n_models_tested": int(len(results)),
            "conditional_volatility": best.get("conditional_volatility"),
            "returns": r,
            "annualized": bool(annualize),
        }

    # =========================================================================
    # REGIME DETECTION
    # =========================================================================
    
    def detect_regimes(
        self,
        returns: pd.Series,
        n_regimes: int = 3,
        features: List[str] = None
    ) -> Dict[str, Any]:
        """Detect market regimes using HMM"""
        if not dep_manager.is_available('hmmlearn'):
            return {'available': False, 'message': 'HMM package not available'}
        
        if features is None:
            features = ['returns', 'volatility', 'volume']
        
        returns_clean = returns.dropna()
        
        if len(returns_clean) < 260:
            return {'available': False, 'message': 'Insufficient data for regime detection'}
        
        try:
            # Prepare features
            feature_data = []
            
            if 'returns' in features:
                feature_data.append(returns_clean.values.reshape(-1, 1))
            
            if 'volatility' in features:
                volatility = returns_clean.rolling(window=20).std() * np.sqrt(self.annual_trading_days)
                volatility = volatility.fillna(method='bfill').values.reshape(-1, 1)
                feature_data.append(volatility)
            
            if 'volume' in features and hasattr(returns_clean, 'volume'):
                volume = returns_clean.volume if hasattr(returns_clean, 'volume') else np.ones_like(returns_clean)
                volume = volume.fillna(method='bfill').values.reshape(-1, 1)
                feature_data.append(volume)
            
            # Combine features
            X = np.hstack(feature_data)
            
            # Scale features
            scaler = dep_manager.dependencies['hmmlearn']['StandardScaler']()
            X_scaled = scaler.fit_transform(X)
            
            # Fit HMM
            GaussianHMM = dep_manager.dependencies['hmmlearn']['GaussianHMM']
            model = GaussianHMM(
                n_components=n_regimes,
                covariance_type='full',
                n_iter=1000,
                random_state=42,
                tol=1e-6
            )
            model.fit(X_scaled)
            
            # Predict regimes
            regimes = model.predict(X_scaled)
            regime_probs = model.predict_proba(X_scaled)
            
            # Calculate regime statistics
            regime_stats = []
            for i in range(n_regimes):
                mask = regimes == i
                if mask.sum() > 0:
                    regime_returns = returns_clean[mask]
                    stats = {
                        'regime': i,
                        'frequency': mask.mean() * 100,
                        'mean_return': regime_returns.mean() * 100,
                        'volatility': regime_returns.std() * np.sqrt(self.annual_trading_days) * 100,
                        'sharpe': (regime_returns.mean() / regime_returns.std()) * np.sqrt(self.annual_trading_days) if regime_returns.std() > 0 else 0,
                        'var_95': np.percentile(regime_returns, 5) * 100
                    }
                    regime_stats.append(stats)
            
            # Label regimes
            if regime_stats:
                stats_df = pd.DataFrame(regime_stats).sort_values('mean_return')
                labels = {}
                colors = ['#ef4444', '#f59e0b', '#10b981', '#3b82f6', '#8b5cf6']
                
                for i, (_, row) in enumerate(stats_df.iterrows()):
                    if i == 0:
                        labels[int(row['regime'])] = {'name': 'Bear', 'color': colors[0]}
                    elif i == len(stats_df) - 1:
                        labels[int(row['regime'])] = {'name': 'Bull', 'color': colors[-1]}
                    else:
                        labels[int(row['regime'])] = {'name': f'Neutral {i}', 'color': colors[i]}
            
            return {
                'available': True,
                'regimes': regimes,
                'regime_probs': regime_probs,
                'regime_stats': regime_stats,
                'regime_labels': labels,
                'model': model,
                'features': X_scaled
            }
            
        except Exception as e:
            return {'available': False, 'message': f'Regime detection failed: {str(e)}'}
    
    # =========================================================================
    # RISK METRICS
    # =========================================================================
    
    def calculate_var(
        self,
        returns: pd.Series,
        confidence_level: float = 0.95,
        method: str = "historical",
        horizon: int = 1,
        use_log_aggregation: bool = True
    ) -> Dict[str, Any]:
        """Robust VaR / CVaR(ES) / ES engine (NaN-proof, horizon-aware).

        Fixes common production issues:
        - NaNs in VaR/CVaR/ES from residual NaNs/Infs or tiny effective samples.
        - Incorrect multi-day scaling (sqrt approximation) by computing horizon returns directly.
        - Key mismatches between analytics output and Streamlit UI expectations.

        Returns POSITIVE loss measures in decimal units:
        - VaR: 0.02 means 2% loss
        - CVaR/ES: expected shortfall (positive)
        """
        # Defensive cleaning: numeric, drop inf/nan, stable order, unique index
        try:
            rr = pd.to_numeric(returns, errors="coerce")
        except Exception:
            rr = returns.copy()

        try:
            rr = rr.replace([np.inf, -np.inf], np.nan).dropna()
        except Exception:
            rr = rr.dropna() if hasattr(rr, "dropna") else rr

        try:
            rr = rr[~rr.index.duplicated(keep="last")].sort_index()
        except Exception:
            pass

        if rr is None or getattr(rr, "empty", False):
            return {"success": False, "message": "No valid returns available for VaR.", "n_obs": 0, "horizon": int(horizon)}

        # Horizon aggregation (compute H-day returns explicitly)
        try:
            h = int(horizon)
        except Exception:
            h = 1
        h = max(1, h)

        if h > 1:
            try:
                if use_log_aggregation:
                    # log aggregation is numerically stable: exp(sum(log(1+r))) - 1
                    lr = np.log1p(rr.astype(float))
                    agg = lr.rolling(h).sum()
                    rr_h = np.expm1(agg).dropna()
                else:
                    rr_h = rr.astype(float).rolling(h).sum().dropna()
            except Exception:
                rr_h = rr.copy()
        else:
            rr_h = rr.copy()

        if rr_h is None or getattr(rr_h, "empty", False):
            return {"success": False, "message": "No valid horizon-aggregated returns for VaR.", "n_obs": 0, "horizon": int(h)}

        # Final sanitize (nanquantile safety)
        try:
            rr_h = pd.to_numeric(rr_h, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        except Exception:
            pass

        n = int(len(rr_h))
        if n <= 0:
            return {"success": False, "message": "No valid returns available after cleaning.", "n_obs": 0, "horizon": int(h)}

        method = (method or "historical").lower().strip()
        cl = float(confidence_level) if confidence_level is not None else 0.95
        cl = min(max(cl, 0.50), 0.999)  # clamp to safe range
        alpha = 1.0 - cl  # tail probability (e.g., 0.05 for 95% VaR)

        # Moments (guard against NaN std when n < 2)
        mu = float(rr_h.mean()) if n > 0 else 0.0
        if n >= 2:
            sigma = float(rr_h.std(ddof=1))
            if not np.isfinite(sigma):
                sigma = float(rr_h.std(ddof=0))
        else:
            sigma = 0.0

        if not np.isfinite(mu):
            mu = 0.0
        if not np.isfinite(sigma):
            sigma = 0.0

        warning = ""
        if n < 60:
            warning = f"Small effective sample (n={n}). Results may be unstable."

        var = 0.0
        cvar = 0.0

        try:
            if method == "historical":
                q = float(np.nanquantile(rr_h.values, alpha))
                var = -q
                tail = rr_h[rr_h <= q]
                cvar = -float(np.nanmean(tail.values)) if len(tail) > 0 else float(var)

            elif method == "parametric":
                if sigma < 1e-12:
                    var, cvar = 0.0, 0.0
                else:
                    z = float(stats.norm.ppf(alpha))
                    q = mu + sigma * z
                    var = -(q)
                    pdf = float(stats.norm.pdf(z))
                    cvar = -mu + sigma * (pdf / max(alpha, 1e-12))

            elif method == "modified":
                # Cornish-Fisher adjusted quantile (using empirical skew/excess kurtosis)
                if sigma < 1e-12:
                    var, cvar = 0.0, 0.0
                else:
                    z = float(stats.norm.ppf(alpha))
                    try:
                        s = float(rr_h.skew())
                    except Exception:
                        s = 0.0
                    try:
                        k_ex = float(rr_h.kurtosis())  # pandas: excess kurtosis by default
                    except Exception:
                        k_ex = 0.0
                    if not np.isfinite(s):
                        s = 0.0
                    if not np.isfinite(k_ex):
                        k_ex = 0.0

                    z_cf = (
                        z
                        + (1.0 / 6.0) * (z**2 - 1.0) * s
                        + (1.0 / 24.0) * (z**3 - 3.0 * z) * k_ex
                        - (1.0 / 36.0) * (2.0 * z**3 - 5.0 * z) * (s**2)
                    )
                    q = mu + sigma * z_cf
                    var = -(q)

                    # Robust ES estimate from empirical tail below the adjusted quantile
                    tail = rr_h[rr_h <= q]
                    cvar = -float(np.nanmean(tail.values)) if len(tail) > 0 else float(var)

            else:
                # Unknown method -> default to historical
                q = float(np.nanquantile(rr_h.values, alpha))
                var = -q
                tail = rr_h[rr_h <= q]
                cvar = -float(np.nanmean(tail.values)) if len(tail) > 0 else float(var)

        except Exception as e:
            return {
                "success": False,
                "message": f"VaR computation failed: {e}",
                "method": method,
                "confidence_level": float(cl),
                "n_obs": int(n),
                "horizon": int(h),
                "warning": warning,
            }

        # Final output sanitation
        if not np.isfinite(var) or not np.isfinite(cvar):
            return {
                "success": False,
                "message": "VaR computation produced non-finite output (NaN/Inf). Check return series cleaning/overlap.",
                "method": method,
                "confidence_level": float(cl),
                "n_obs": int(n),
                "horizon": int(h),
                "warning": warning,
            }

        # Ensure non-negative loss magnitudes (can happen if returns are strongly positive)
        var = float(max(var, 0.0))
        cvar = float(max(cvar, 0.0))

        return {
            "success": True,
            "VaR": var,
            "CVaR": cvar,
            "ES": cvar,
            "confidence_level": float(cl),
            "method": method,
            "n_obs": int(n),
            "horizon": int(h),
            "warning": warning,
            "mu": float(mu),
            "sigma": float(sigma),
        }

    def stress_test(
        self,
        returns: pd.Series,
        scenarios: List[float] = None,
        shock: Optional[float] = None,
        duration: int = 1
    ) -> Dict[str, Any]:
        """Perform stress testing.

        Supports two modes (backward compatible):
        1) Scenario grid: pass `scenarios=[...]` (default) to apply additive return shocks and report metrics.
        2) Single shock path: pass `shock=<total shock>` and `duration=<days>` to distribute the total shock
           over the first `duration` observations (compounded) and return a simulated path.

        Notes:
        - If your UI passes `shock=` and `duration=`, this method will not raise an error.
        - min length / data quality checks are handled upstream in UI; this method is defensive anyway.
        """
        # Defensive clean-up
        try:
            returns_clean = pd.to_numeric(returns, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        except Exception:
            returns_clean = returns.dropna()

        if returns_clean is None or len(returns_clean) == 0:
            return {"success": False, "message": "No valid returns provided for stress test."}

        # Base (unshocked) path for reference
        base_path = (1.0 + returns_clean).cumprod()

        # Mode 2: single shock path (used by your Streamlit UI)
        if shock is not None:
            try:
                shock_total = float(shock)
            except Exception:
                return {"success": False, "message": f"Invalid shock value: {shock}"}

            try:
                dur = max(1, int(duration))
            except Exception:
                dur = 1

            # Convert total shock into an equivalent per-day compounded shock
            try:
                daily_shock = (1.0 + shock_total) ** (1.0 / float(dur)) - 1.0
            except Exception:
                daily_shock = shock_total / float(dur)

            shocked = returns_clean.copy()
            k = min(dur, len(shocked))
            if k > 0:
                shocked.iloc[:k] = shocked.iloc[:k] + daily_shock

            path = (1.0 + shocked).cumprod()
            try:
                metrics = self.calculate_performance_metrics(shocked)
            except Exception as e:
                metrics = {"error": str(e)}

            return {
                "success": True,
                "mode": "single_shock",
                "shock_total": shock_total,
                "duration_days": dur,
                "daily_shock": float(daily_shock),
                "metrics": metrics,
                "path": path,
                "base_path": base_path
            }

        # Mode 1: scenario grid (legacy/default)
        if scenarios is None:
            scenarios = [-0.01, -0.02, -0.05, -0.10]

        results = {}
        for sc in scenarios:
            try:
                sc = float(sc)
            except Exception:
                continue
            shocked_returns = returns_clean + sc
            try:
                results[str(sc)] = self.calculate_performance_metrics(shocked_returns)
            except Exception as e:
                results[str(sc)] = {"error": str(e)}

        return {
            "success": True,
            "mode": "scenario_grid",
            "scenarios": list(scenarios),
            "results": results,
            "base_path": base_path
        }


    def monte_carlo_simulation(
        self,
        returns: pd.Series,
        n_simulations: int = 10000,
        n_days: int = 252
    ) -> Dict[str, Any]:
        """Perform Monte Carlo simulation for returns"""
        returns_clean = returns.dropna()
        
        if len(returns_clean) < 60:
            return {}
        
        mean = returns_clean.mean()
        std = returns_clean.std()
        
        # Generate random returns
        np.random.seed(42)
        simulated_returns = np.random.normal(mean, std, (n_simulations, n_days))
        
        # Calculate paths
        paths = 100 * np.cumprod(1 + simulated_returns, axis=1)
        
        # Calculate statistics
        final_values = paths[:, -1]
        max_values = paths.max(axis=1)
        min_values = paths.min(axis=1)
        
        return {
            'paths': paths,
            'mean_final_value': np.mean(final_values),
            'std_final_value': np.std(final_values),
            'var_95_final': np.percentile(final_values, 5),
            'cvar_95_final': final_values[final_values <= np.percentile(final_values, 5)].mean(),
            'probability_loss': (final_values < 100).mean() * 100,
            'expected_max': np.mean(max_values),
            'expected_min': np.mean(min_values)
        }

# =============================================================================
# ADVANCED VISUALIZATION ENGINE
# =============================================================================

class InstitutionalVisualizer:
    """Professional visualization engine for institutional analytics"""
    
    def __init__(self, theme: str = "default"):
        self.theme = theme
        self.colors = ThemeManager.THEMES.get(theme, ThemeManager.THEMES["default"])
        
        # Plotly template
        self.template = go.layout.Template(
            layout=go.Layout(
                font_family="Inter, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif",
                title_font_size=20,
                title_font_color=self.colors['dark'],
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                hovermode='x unified',
                hoverlabel=dict(
                    bgcolor=self.colors['dark'],
                    font_size=12,
                    font_family="Inter"
                ),
                colorway=[self.colors['primary'], self.colors['secondary'], 
                         self.colors['accent'], self.colors['success'],
                         self.colors['warning'], self.colors['danger']],
                xaxis=dict(
                    gridcolor='rgba(0,0,0,0.1)',
                    gridwidth=1,
                    zerolinecolor='rgba(0,0,0,0.1)',
                    zerolinewidth=1
                ),
                yaxis=dict(
                    gridcolor='rgba(0,0,0,0.1)',
                    gridwidth=1,
                    zerolinecolor='rgba(0,0,0,0.1)',
                    zerolinewidth=1
                ),
                legend=dict(
                    bgcolor='rgba(255,255,255,0.9)',
                    bordercolor='rgba(0,0,0,0.1)',
                    borderwidth=1,
                    font_size=12
                ),
                margin=dict(l=50, r=50, t=80, b=50)
            )
        )
    
    def create_price_chart(
        self,
        df: pd.DataFrame,
        title: str,
        show_indicators: bool = True
    ) -> go.Figure:
        """Create comprehensive price chart with technical indicators"""
        
        price_col = 'Adj_Close' if 'Adj_Close' in df.columns else 'Close'
        
        # Determine subplot configuration
        if show_indicators:
            fig = make_subplots(
                rows=4, cols=1,
                shared_xaxes=True,
                vertical_spacing=0.05,
                row_heights=[0.5, 0.15, 0.15, 0.2],
                subplot_titles=(
                    f"{title} - Price Action",
                    "Volume",
                    "RSI",
                    "MACD"
                )
            )
        else:
            fig = make_subplots(
                rows=2, cols=1,
                shared_xaxes=True,
                vertical_spacing=0.05,
                row_heights=[0.7, 0.3],
                subplot_titles=(f"{title} - Price Action", "Volume")
            )
        
        # Price and moving averages
        fig.add_trace(
            go.Scatter(
                x=df.index,
                y=df[price_col],
                name='Price',
                line=dict(color=self.colors['primary'], width=2),
                fill='tozeroy',
                fillcolor=f"rgba({int(self.colors['primary'][1:3], 16)}, "
                         f"{int(self.colors['primary'][3:5], 16)}, "
                         f"{int(self.colors['primary'][5:7], 16)}, 0.1)"
            ),
            row=1, col=1
        )
        
        # Moving averages
        for period, color in [(20, self.colors['secondary']), (50, self.colors['accent'])]:
            if f'SMA_{period}' in df.columns:
                fig.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df[f'SMA_{period}'],
                        name=f'SMA {period}',
                        line=dict(color=color, width=1.5, dash='dash'),
                        opacity=0.7
                    ),
                    row=1, col=1
                )
        
        # Bollinger Bands
        if all(col in df.columns for col in ['BB_Upper', 'BB_Lower']):
            fig.add_trace(
                go.Scatter(
                    x=df.index,
                    y=df['BB_Upper'],
                    name='BB Upper',
                    line=dict(color=self.colors['gray'], width=1, dash='dot'),
                    opacity=0.5,
                    showlegend=False
                ),
                row=1, col=1
            )
            
            fig.add_trace(
                go.Scatter(
                    x=df.index,
                    y=df['BB_Lower'],
                    name='BB Lower',
                    line=dict(color=self.colors['gray'], width=1, dash='dot'),
                    opacity=0.5,
                    showlegend=False,
                    fill='tonexty',
                    fillcolor=f"rgba({int(self.colors['gray'][1:3], 16)}, "
                             f"{int(self.colors['gray'][3:5], 16)}, "
                             f"{int(self.colors['gray'][5:7], 16)}, 0.1)"
                ),
                row=1, col=1
            )
        
        # Volume
        if 'Volume' in df.columns:
            colors = [self.colors['success'] if close >= open_ else self.colors['danger']
                     for close, open_ in zip(df[price_col], df['Open'])]
            
            fig.add_trace(
                go.Bar(
                    x=df.index,
                    y=df['Volume'],
                    name='Volume',
                    marker_color=colors,
                    opacity=0.7
                ),
                row=2 if show_indicators else 2, col=1
            )
        
        # RSI
        if show_indicators and 'RSI' in df.columns:
            fig.add_trace(
                go.Scatter(
                    x=df.index,
                    y=df['RSI'],
                    name='RSI',
                    line=dict(color=self.colors['accent'], width=2)
                ),
                row=3, col=1
            )
            
            # Add RSI bands
            fig.add_hline(y=70, line_dash="dash", line_color=self.colors['danger'],
                         opacity=0.5, row=3, col=1)
            fig.add_hline(y=30, line_dash="dash", line_color=self.colors['success'],
                         opacity=0.5, row=3, col=1)
            fig.add_hline(y=50, line_dash="dot", line_color=self.colors['gray'],
                         opacity=0.3, row=3, col=1)
        
        # MACD
        if show_indicators and all(col in df.columns for col in ['MACD', 'MACD_Signal', 'MACD_Histogram']):
            fig.add_trace(
                go.Scatter(
                    x=df.index,
                    y=df['MACD'],
                    name='MACD',
                    line=dict(color=self.colors['primary'], width=2)
                ),
                row=4, col=1
            )
            
            fig.add_trace(
                go.Scatter(
                    x=df.index,
                    y=df['MACD_Signal'],
                    name='Signal',
                    line=dict(color=self.colors['secondary'], width=2)
                ),
                row=4, col=1
            )
            
            # Histogram
            colors = [self.colors['success'] if x >= 0 else self.colors['danger']
                     for x in df['MACD_Histogram']]
            
            fig.add_trace(
                go.Bar(
                    x=df.index,
                    y=df['MACD_Histogram'],
                    name='Histogram',
                    marker_color=colors,
                    opacity=0.6
                ),
                row=4, col=1
            )
        
        # Update layout
        fig.update_layout(
            title=dict(
                text=title,
                x=0.5,
                font=dict(size=24, color=self.colors['dark'])
            ),
            height=900 if show_indicators else 700,
            template=self.template,
            showlegend=True,
            hovermode='x unified'
        )
        
        # Update axes
        fig.update_yaxes(title_text="Price ($)", row=1, col=1)
        fig.update_yaxes(title_text="Volume", row=2 if show_indicators else 2, col=1)
        
        if show_indicators:
            fig.update_yaxes(title_text="RSI", row=3, col=1, range=[0, 100])
            fig.update_yaxes(title_text="MACD", row=4, col=1)
        
        return fig
    
    def create_performance_chart(
        self,
        returns: Union[pd.Series, pd.DataFrame],
        benchmark_returns: Optional[pd.Series] = None,
        title: str = "Performance Analysis"
    ) -> go.Figure:
        """Create performance visualization with multiple metrics.

        Robustly supports both pd.Series (single strategy/portfolio) and pd.DataFrame
        (multi-asset or multi-strategy) inputs.
        """

        # -----------------------------
        # Normalize input -> DataFrame
        # -----------------------------
        if returns is None:
            returns_df = pd.DataFrame()
        elif isinstance(returns, pd.DataFrame):
            returns_df = returns.copy()
        else:
            name = getattr(returns, "name", None) or "Portfolio"
            returns_df = pd.DataFrame({name: returns})

        # Coerce to numeric and drop empty rows/cols safely
        if not returns_df.empty:
            returns_df = returns_df.apply(pd.to_numeric, errors="coerce")
            returns_df = returns_df.dropna(how="all")
            returns_df = returns_df.dropna(axis=1, how="all")

        # Align benchmark to returns index (if present)
        bmk = None
        if benchmark_returns is not None:
            try:
                bmk = pd.to_numeric(benchmark_returns, errors="coerce").dropna()
                if (bmk is not None) and (not returns_df.empty):
                    common_idx = returns_df.index.intersection(bmk.index)
                    returns_df = returns_df.loc[common_idx]
                    bmk = bmk.loc[common_idx]
            except Exception:
                bmk = None

        fig = make_subplots(
            rows=3, cols=2,
            subplot_titles=(
                "Cumulative Returns",
                "Drawdown",
                "Rolling Returns (12M)",
                "Rolling Volatility (12M)",
                "Returns Distribution",
                "QQ Plot"
            ),
            specs=[
                [{"type": "scatter"}, {"type": "scatter"}],
                [{"type": "scatter"}, {"type": "scatter"}],
                [{"type": "histogram"}, {"type": "scatter"}]
            ]
        )

        cols = list(returns_df.columns) if not returns_df.empty else []
        palette = [
            self.colors.get("primary", "#1f77b4"),
            self.colors.get("secondary", "#ff7f0e"),
            self.colors.get("success", "#2ca02c"),
            self.colors.get("warning", "#d62728"),
            self.colors.get("danger", "#9467bd"),
            self.colors.get("gray", "#7f7f7f"),
        ]

        # -----------------------------
        # Cumulative returns (row 1, col 1)
        # -----------------------------
        for i, col in enumerate(cols):
            s = returns_df[col].dropna()
            if s.empty:
                continue
            cumulative = (1 + s).cumprod()
            fig.add_trace(
                go.Scatter(
                    x=cumulative.index,
                    y=cumulative.values,
                    name=str(col),
                    line=dict(color=palette[i % len(palette)], width=3 if len(cols) == 1 else 2),
                    fill='tozeroy' if len(cols) == 1 else None,
                ),
                row=1, col=1
            )

        if bmk is not None and len(bmk) > 0:
            benchmark_cumulative = (1 + bmk).cumprod()
            fig.add_trace(
                go.Scatter(
                    x=benchmark_cumulative.index,
                    y=benchmark_cumulative.values,
                    name="Benchmark",
                    line=dict(color=self.colors.get("gray", "#888888"), width=2, dash='dash')
                ),
                row=1, col=1
            )

        # -----------------------------
        # Drawdown (row 1, col 2)
        # -----------------------------
        for i, col in enumerate(cols):
            s = returns_df[col].dropna()
            if s.empty:
                continue
            cumulative = (1 + s).cumprod()
            running_max = cumulative.cummax()
            drawdown = (cumulative - running_max) / running_max * 100
            fig.add_trace(
                go.Scatter(
                    x=drawdown.index,
                    y=drawdown.values,
                    name=f"{col} Drawdown" if len(cols) > 1 else "Drawdown",
                    line=dict(color=palette[i % len(palette)], width=2),
                    fill='tozeroy' if len(cols) == 1 else None,
                    opacity=0.85 if len(cols) > 1 else 0.95
                ),
                row=1, col=2
            )

        if bmk is not None and len(bmk) > 0:
            bc = (1 + bmk).cumprod()
            rm = bc.cummax()
            bdd = (bc - rm) / rm * 100
            fig.add_trace(
                go.Scatter(
                    x=bdd.index,
                    y=bdd.values,
                    name="Benchmark Drawdown",
                    line=dict(color=self.colors.get("gray", "#888888"), width=2, dash='dot'),
                    opacity=0.9
                ),
                row=1, col=2
            )

        # -----------------------------
        # Rolling returns (row 2, col 1)
        # -----------------------------
        for i, col in enumerate(cols):
            s = returns_df[col]
            rolling_returns = s.rolling(window=252, min_periods=60).mean() * 252 * 100
            fig.add_trace(
                go.Scatter(
                    x=rolling_returns.index,
                    y=rolling_returns.values,
                    name=f"{col} Rolling Return" if len(cols) > 1 else "Rolling Return",
                    line=dict(color=palette[i % len(palette)], width=2),
                    opacity=0.75 if len(cols) > 1 else 0.95
                ),
                row=2, col=1
            )

        if bmk is not None and len(bmk) > 0:
            brr = bmk.rolling(window=252, min_periods=60).mean() * 252 * 100
            fig.add_trace(
                go.Scatter(
                    x=brr.index,
                    y=brr.values,
                    name="Benchmark Rolling Return",
                    line=dict(color=self.colors.get("gray", "#888888"), width=2, dash='dash')
                ),
                row=2, col=1
            )

        # -----------------------------
        # Rolling volatility (row 2, col 2)
        # -----------------------------
        for i, col in enumerate(cols):
            s = returns_df[col]
            rolling_vol = s.rolling(window=252, min_periods=60).std() * np.sqrt(252) * 100
            fig.add_trace(
                go.Scatter(
                    x=rolling_vol.index,
                    y=rolling_vol.values,
                    name=f"{col} Rolling Vol" if len(cols) > 1 else "Rolling Volatility",
                    line=dict(color=palette[i % len(palette)], width=2),
                    opacity=0.75 if len(cols) > 1 else 0.95
                ),
                row=2, col=2
            )

        if bmk is not None and len(bmk) > 0:
            brv = bmk.rolling(window=252, min_periods=60).std() * np.sqrt(252) * 100
            fig.add_trace(
                go.Scatter(
                    x=brv.index,
                    y=brv.values,
                    name="Benchmark Rolling Vol",
                    line=dict(color=self.colors.get("gray", "#888888"), width=2, dash='dash')
                ),
                row=2, col=2
            )

        # -----------------------------
        # Returns distribution (row 3, col 1)
        # -----------------------------
        for i, col in enumerate(cols):
            s = (returns_df[col] * 100).dropna()
            if s.empty:
                continue
            fig.add_trace(
                go.Histogram(
                    x=s,
                    nbinsx=50,
                    name=str(col),
                    marker_color=palette[i % len(palette)],
                    opacity=0.45 if len(cols) > 1 else 0.7
                ),
                row=3, col=1
            )

        # -----------------------------
        # QQ Plot (row 3, col 2) - per series + pooled theoretical line
        # -----------------------------
        for i, col in enumerate(cols):
            vals = returns_df[col].dropna().values
            if vals is None or len(vals) <= 10:
                continue
            try:
                qq_data = stats.probplot(vals, dist="norm")
                fig.add_trace(
                    go.Scatter(
                        x=qq_data[0][0],
                        y=qq_data[0][1],
                        mode='markers',
                        name=str(col),
                        marker=dict(size=6),
                        opacity=0.7 if len(cols) > 1 else 1.0
                    ),
                    row=3, col=2
                )
            except Exception:
                continue

        # Add pooled theoretical line (prevents DataFrame probplot shape issues)
        try:
            pooled = returns_df.stack().dropna().values if not returns_df.empty else np.array([])
            if pooled is not None and len(pooled) > 10:
                qq_all = stats.probplot(pooled, dist="norm")
                x_line = np.array([qq_all[0][0][0], qq_all[0][0][-1]])
                y_line = qq_all[1][0] + qq_all[1][1] * x_line
                fig.add_trace(
                    go.Scatter(
                        x=x_line,
                        y=y_line,
                        mode='lines',
                        name="Normal",
                        line=dict(color=self.colors.get("danger", "#d62728"), width=2, dash='dash')
                    ),
                    row=3, col=2
                )
        except Exception:
            pass

        # Update layout
        fig.update_layout(
            title=dict(text=title, x=0.5, font=dict(size=24)),
            height=1000,
            template=self.template,
            showlegend=True,
            hovermode='x unified'
        )

        # Update axes titles (consistent with subplot placement)
        fig.update_yaxes(title_text="Cumulative Return", row=1, col=1)
        fig.update_yaxes(title_text="Drawdown (%)", row=1, col=2)
        fig.update_yaxes(title_text="Annual Return (%)", row=2, col=1)
        fig.update_yaxes(title_text="Annual Volatility (%)", row=2, col=2)
        fig.update_yaxes(title_text="Count", row=3, col=1)
        fig.update_yaxes(title_text="Sample Quantiles", row=3, col=2)

        fig.update_xaxes(title_text="Date", row=1, col=1)
        fig.update_xaxes(title_text="Date", row=1, col=2)
        fig.update_xaxes(title_text="Date", row=2, col=1)
        fig.update_xaxes(title_text="Date", row=2, col=2)
        fig.update_xaxes(title_text="Return (%)", row=3, col=1)
        fig.update_xaxes(title_text="Theoretical Quantiles", row=3, col=2)

        return fig
    def create_correlation_matrix(
        self,
        corr_matrix: pd.DataFrame,
        title: str = "Correlation Matrix"
    ) -> go.Figure:
        """Create interactive correlation heatmap"""
        
        fig = go.Figure(data=go.Heatmap(
            z=corr_matrix.values,
            x=corr_matrix.columns,
            y=corr_matrix.index,
            colorscale='RdBu',
            zmid=0,
            zmin=-1,
            zmax=1,
            text=corr_matrix.round(2).values,
            texttemplate='%{text}',
            hoverinfo='x+y+z',
	            # Plotly Heatmap ColorBar does NOT support a top-level `titleside`.
	            # Some snippets online use `titleside`, but it will raise on
	            # Streamlit Cloud's Plotly versions.
	            # Use the supported nested form: colorbar.title.text.
	            colorbar=dict(
	                title=dict(text='Correlation'),
	                tickformat='.2f'
	            )
        ))
        
        fig.update_layout(
            title=dict(text=title, x=0.5, font=dict(size=20)),
            height=600,
            width=max(800, len(corr_matrix.columns) * 100),
            template=self.template,
            xaxis_tickangle=45,
            xaxis=dict(side="bottom"),
            yaxis=dict(autorange="reversed")
        )
        
        return fig
    
    def create_risk_decomposition(
        self,
        risk_contributions: Dict[str, float],
        title: str = "Risk Contribution Breakdown"
    ) -> go.Figure:
        """Create risk decomposition visualization"""
        
        labels = list(risk_contributions.keys())
        values = list(risk_contributions.values())
        
        fig = go.Figure(data=[go.Sunburst(
            labels=labels,
            parents=[''] * len(labels),
            values=values,
            branchvalues="total",
            marker=dict(
                colors=px.colors.qualitative.Set3,
                line=dict(color='white', width=2)
            ),
            hovertemplate='<b>%{label}</b><br>Risk Contribution: %{value:.1f}%<br>',
            textinfo='label+percent entry'
        )])
        
        fig.update_layout(
            title=dict(text=title, x=0.5, font=dict(size=20)),
            height=500,
            template=self.template,
            margin=dict(t=50, l=0, r=0, b=0)
        )
        
        return fig
    
    def create_regime_chart(
        self,
        price: pd.Series,
        regimes: np.ndarray,
        regime_labels: Dict[int, Dict],
        title: str = "Market Regimes"
    ) -> go.Figure:
        """Create regime visualization"""
        
        fig = go.Figure()
        
        # Plot price
        fig.add_trace(go.Scatter(
            x=price.index,
            y=price.values,
            name='Price',
            line=dict(color=self.colors['gray'], width=1),
            opacity=0.7
        ))
        
        # Add regime highlights
        unique_regimes = np.unique(regimes)
        
        for regime in unique_regimes:
            mask = regimes == regime
            regime_dates = price.index[mask]
            regime_prices = price.values[mask]
            
            label_info = regime_labels.get(int(regime), {'name': f'Regime {regime}', 'color': self.colors['gray']})
            
            fig.add_trace(go.Scatter(
                x=regime_dates,
                y=regime_prices,
                mode='markers',
                name=label_info['name'],
                marker=dict(
                    size=8,
                    color=label_info['color'],
                    symbol='circle',
                    line=dict(width=1, color='white')
                ),
                opacity=0.8
            ))
        
        fig.update_layout(
            title=dict(text=title, x=0.5, font=dict(size=20)),
            height=500,
            template=self.template,
            hovermode='x unified',
            yaxis_title="Price",
            xaxis_title="Date"
        )
        
        return fig
    
    def create_garch_volatility(
        self,
        returns: pd.Series,
        conditional_vol: np.ndarray,
        forecast_vol: Optional[np.ndarray] = None,
        title: str = "GARCH Volatility Analysis"
    ) -> go.Figure:
        """Create GARCH volatility visualization"""
        
        fig = go.Figure()
        
        # Realized volatility
        realized_vol = returns.rolling(window=20).std() * np.sqrt(252) * 100
        
        fig.add_trace(go.Scatter(
            x=realized_vol.index,
            y=realized_vol.values,
            name='Realized Vol (20D)',
            line=dict(color=self.colors['gray'], width=2),
            opacity=0.7
        ))
        
        # Conditional volatility
        if conditional_vol is not None:
            cond_vol_series = pd.Series(conditional_vol * 100, index=returns.index[:len(conditional_vol)])
            fig.add_trace(go.Scatter(
                x=cond_vol_series.index,
                y=cond_vol_series.values,
                name='GARCH Conditional Vol',
                line=dict(color=self.colors['primary'], width=3)
            ))
        
        # Forecast volatility
        if forecast_vol is not None:
            forecast_dates = pd.date_range(
                start=returns.index[-1] + pd.Timedelta(days=1),
                periods=len(forecast_vol),
                freq='D'
            )
            fig.add_trace(go.Scatter(
                x=forecast_dates,
                y=forecast_vol * 100,
                name='Volatility Forecast',
                line=dict(color=self.colors['danger'], width=2, dash='dot')
            ))
        
        fig.update_layout(
            title=dict(text=title, x=0.5, font=dict(size=20)),
            height=500,
            template=self.template,
            hovermode='x unified',
            yaxis_title="Annualized Volatility (%)",
            xaxis_title="Date"
        )
        
        return fig

    def create_ewma_ratio_signal_chart(
        self,
        ewma_df: pd.DataFrame,
        title: str = "EWMA Volatility Ratio Signal",
        bb_window: int = 20,
        bb_k: float = 2.0,
        green_max: float = 0.35,
        red_min: float = 0.55,
        show_bollinger: bool = True,
        show_threshold_lines: bool = True
    ) -> go.Figure:
        """Create an institutional EWMA ratio chart with Bollinger Bands + alarm zones.

        Zones:
            GREEN  : ratio <= green_max
            ORANGE : green_max < ratio < red_min
            RED    : ratio >= red_min
        """
        df = ewma_df.copy()
        if df.empty or "EWMA_RATIO" not in df.columns:
            fig = go.Figure()
            fig.update_layout(
                title=dict(text=title, x=0.5),
                height=520,
                template=self.template
            )
            return fig

        ratio = pd.to_numeric(df["EWMA_RATIO"], errors="coerce").dropna()
        if ratio.empty:
            fig = go.Figure()
            fig.update_layout(
                title=dict(text=title, x=0.5),
                height=520,
                template=self.template
            )
            return fig

        # Bollinger on ratio (rolling)
        bb_window = int(max(5, bb_window))
        bb_k = float(bb_k)

        mid = ratio.rolling(window=bb_window, min_periods=max(5, bb_window//2)).mean()
        std = ratio.rolling(window=bb_window, min_periods=max(5, bb_window//2)).std()
        upper = (mid + bb_k * std).rename("BB_UPPER")
        lower = (mid - bb_k * std).rename("BB_LOWER")

        # Determine y-range for colored zones
        y_min = float(max(0.0, np.nanmin([ratio.min(), lower.min() if not lower.dropna().empty else ratio.min()])))
        y_max = float(np.nanmax([ratio.max(), upper.max() if not upper.dropna().empty else ratio.max()]))
        y_pad = 0.15 * (y_max - y_min) if y_max > y_min else 0.1
        y_top = y_max + y_pad

        x0 = ratio.index.min()
        x1 = ratio.index.max()

        # Zone levels sanity
        green_max = float(green_max)
        red_min = float(red_min)
        if red_min <= green_max:
            red_min = green_max + 1e-6

        fig = go.Figure()

        # Add shaded bands (risk signal)
        fig.add_shape(
            type="rect",
            xref="x", yref="y",
            x0=x0, x1=x1,
            y0=y_min, y1=green_max,
            fillcolor=self.colors.get("success", "#10b981"),
            opacity=0.10,
            line_width=0,
            layer="below"
        )
        fig.add_shape(
            type="rect",
            xref="x", yref="y",
            x0=x0, x1=x1,
            y0=green_max, y1=red_min,
            fillcolor=self.colors.get("warning", "#f59e0b"),
            opacity=0.10,
            line_width=0,
            layer="below"
        )
        fig.add_shape(
            type="rect",
            xref="x", yref="y",
            x0=x0, x1=x1,
            y0=red_min, y1=y_top,
            fillcolor=self.colors.get("danger", "#ef4444"),
            opacity=0.10,
            line_width=0,
            layer="below"
        )

        # Ratio line
        fig.add_trace(
            go.Scatter(
                x=ratio.index,
                y=ratio.values,
                name="EWMA Ratio",
                mode="lines",
                line=dict(color=self.colors.get("primary", "#1a2980"), width=2.5)
            )
        )

        if show_bollinger:
            fig.add_trace(
                go.Scatter(
                    x=mid.index,
                    y=mid.values,
                    name=f"BB Mid ({bb_window})",
                    mode="lines",
                    line=dict(color=self.colors.get("secondary", "#26d0ce"), width=2, dash="dot"),
                    opacity=0.9
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=upper.index,
                    y=upper.values,
                    name="BB Upper",
                    mode="lines",
                    line=dict(color=self.colors.get("warning", "#f59e0b"), width=2, dash="dash"),
                    opacity=0.9
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=lower.index,
                    y=lower.values,
                    name="BB Lower",
                    mode="lines",
                    line=dict(color=self.colors.get("warning", "#f59e0b"), width=2, dash="dash"),
                    opacity=0.9
                )
            )

        if show_threshold_lines:
            fig.add_hline(
                y=green_max,
                line_dash="dash",
                line_color=self.colors.get("success", "#10b981"),
                opacity=0.7
            )
            fig.add_hline(
                y=red_min,
                line_dash="dash",
                line_color=self.colors.get("danger", "#ef4444"),
                opacity=0.7
            )

        # Latest marker with status color
        last_x = ratio.index[-1]
        last_y = float(ratio.iloc[-1])
        if last_y <= green_max:
            mcol = self.colors.get("success", "#10b981")
            status = "GREEN"
        elif last_y >= red_min:
            mcol = self.colors.get("danger", "#ef4444")
            status = "RED"
        else:
            mcol = self.colors.get("warning", "#f59e0b")
            status = "ORANGE"

        fig.add_trace(
            go.Scatter(
                x=[last_x],
                y=[last_y],
                name=f"Latest ({status})",
                mode="markers",
                marker=dict(size=10, color=mcol, symbol="diamond")
            )
        )

        fig.update_layout(
            title=dict(text=title, x=0.5, font=dict(size=20)),
            height=560,
            template=self.template,
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            margin=dict(l=40, r=40, t=70, b=50)
        )

        fig.update_yaxes(title_text="Ratio", range=[y_min, y_top])
        fig.update_xaxes(title_text="Date", rangeslider=dict(visible=True))

        return fig

# =============================================================================
# INSTITUTIONAL DASHBOARD
# =============================================================================

class InstitutionalCommoditiesDashboard:
    """Main dashboard class with superior architecture"""
    
    def __init__(self):
        # Initialize components
        self.data_manager = EnhancedDataManager()
        self.analytics = InstitutionalAnalytics()
        self.visualizer = InstitutionalVisualizer()
        
        # Initialize session state
        self._init_session_state()
        
        # Performance tracking
        self.start_time = datetime.now()
    
    def _init_session_state(self):
        """Initialize comprehensive session state"""
        defaults = {
            # Data state
            'data_loaded': False,
            'selected_assets': [],
            'selected_benchmarks': [],
            'asset_data': {},
            'benchmark_data': {},
            'returns_data': {},
            'feature_data': {},
            
            # Portfolio state
            'portfolio_weights': {},
            'portfolio_metrics': {},
            'optimization_results': {},
            
            # Analysis state
            'garch_results': {},
            'regime_results': {},
            'risk_results': {},
            'monte_carlo_results': {},
            
            # Configuration
            'analysis_config': AnalysisConfiguration(
                start_date=datetime.now() - timedelta(days=1095),
                end_date=datetime.now()
            ),
            
            # UI state
            'current_tab': 'dashboard',
            'last_update': datetime.now(),
            'error_log': []
        }
        
        for key, value in defaults.items():
            if key not in st.session_state:
                st.session_state[key] = value
    
    def _log_error(self, error: Exception, context: str = ""):
        """Log errors for debugging"""
        error_entry = {
            'timestamp': datetime.now(),
            'error': str(error),
            'context': context,
            'traceback': traceback.format_exc()
        }
        st.session_state.error_log.append(error_entry)


    def _safe_data_points(self, returns_data) -> int:
        """Safely compute number of observations in returns_data (DataFrame/Series/dict/array).

        Streamlit session_state may store returns either as a DataFrame (preferred) or a dict of series/frames.
        This helper avoids ambiguous truth checks and '.values()' call mistakes.
        """
        try:
            if returns_data is None:
                return 0

            # Dict of returns series/frames
            if isinstance(returns_data, dict):
                if len(returns_data) == 0:
                    return 0
                first = next(iter(returns_data.values()), None)
                if first is None:
                    return 0
                if isinstance(first, (pd.DataFrame, pd.Series)):
                    return 0 if first.empty else int(first.shape[0])
                try:
                    return int(len(first))
                except Exception:
                    return 0

            # Pandas objects
            if isinstance(returns_data, pd.DataFrame):
                return 0 if returns_data.empty else int(returns_data.shape[0])
            if isinstance(returns_data, pd.Series):
                return 0 if returns_data.empty else int(returns_data.shape[0])

            # Numpy arrays / lists
            if hasattr(returns_data, "shape") and returns_data.shape is not None:
                shp = returns_data.shape
                return int(shp[0]) if len(shp) >= 1 else 0

            return int(len(returns_data))
        except Exception:
            return 0
    
    # =========================================================================
    # HEADER & SIDEBAR
    # =========================================================================
    

    def display_header(self):
        """Display professional institutional header (clean)."""

        st.components.v1.html(f"""
        <div style="
            background: linear-gradient(135deg, #1a2980 0%, #26d0ce 100%);
            padding: 1.6rem 1.8rem;
            border-radius: 12px;
            color: #ffffff;
            margin-bottom: 1.25rem;
            box-shadow: 0 8px 25px rgba(0,0,0,0.12);
        ">
            <div style="font-size:2.25rem; font-weight:850; line-height:1.15;">
                🏛️ Institutional Commodities Analytics v6.0
            </div>
        </div>
        """, height=115)




    def _render_sidebar_controls(self):
        """Sidebar: universe/asset selection + dates + load button."""
        with st.sidebar:
            st.markdown("## ⚙️ Controls")

            with st.expander("System", expanded=False):
                st.checkbox(
                    "Show system diagnostics",
                    key="show_system_diagnostics",
                    value=False,
                    help="When enabled, shows optional dependency notices and low-level system warnings."
                )

            # --- Universe / Asset selection ---
            categories = list(COMMODITIES_UNIVERSE.keys())
            # Prefer common defaults if available
            preferred_defaults = [
                AssetCategory.PRECIOUS_METALS.value,
                AssetCategory.ENERGY.value,
            ]
            default_categories = [c for c in preferred_defaults if c in categories] or (categories[:2] if categories else [])
            selected_categories = st.multiselect(
                "Commodity Groups",
                options=categories,
                default=default_categories,
                key="sidebar_groups",
                help="Select one or more commodity groups to populate the asset list."
            )

            ticker_to_label = {}
            for cat in selected_categories:
                for t, meta in COMMODITIES_UNIVERSE.get(cat, {}).items():
                    ticker_to_label[t] = f"{t} — {getattr(meta, 'name', str(t))}"

            asset_options = list(ticker_to_label.keys())
            preferred_assets = ["GC=F", "SI=F", "CL=F", "HG=F"]
            default_assets = [t for t in preferred_assets if t in asset_options]
            if not default_assets and asset_options:
                default_assets = asset_options[: min(4, len(asset_options))]

            selected_assets = st.multiselect(
                "Assets",
                options=asset_options,
                default=default_assets,
                format_func=lambda x: ticker_to_label.get(x, x),
                key="sidebar_assets",
                help="Select the assets to analyze."
            )

            # --- Benchmarks ---
            bench_options = list(BENCHMARKS.keys())
            bench_to_label = {k: f"{k} — { (v.get('name','') if isinstance(v, dict) else getattr(v, 'name', str(v))) }" for k, v in BENCHMARKS.items()}
            preferred_bench = ["SPY", "BCOM", "DBC"]
            default_bench = [b for b in preferred_bench if b in bench_options][:1] or (bench_options[:1] if bench_options else [])
            selected_benchmarks = st.multiselect(
                "Benchmarks",
                options=bench_options,
                default=default_bench,
                format_func=lambda x: bench_to_label.get(x, x),
                key="sidebar_benchmarks",
                help="Select one or more benchmarks for relative metrics."
            )

            st.markdown("---")

            # --- Dates ---
            today = datetime.now().date()
            default_start = today - timedelta(days=365 * 2)

            # Persist dates across reruns
            prev_cfg = st.session_state.get("analysis_config", None)
            prev_start = getattr(prev_cfg, "start_date", None)
            prev_end = getattr(prev_cfg, "end_date", None)

            c1, c2 = st.columns(2)
            start_date = c1.date_input(
                "Start",
                value=(prev_start.date() if prev_start else default_start),
                key="sidebar_start_date"
            )
            end_date = c2.date_input(
                "End",
                value=(prev_end.date() if prev_end else today),
                key="sidebar_end_date"
            )

            # --- Runtime / actions ---
            auto_reload = st.checkbox(
                "Auto-reload on changes",
                value=False,
                key="sidebar_autoreload",
                help="If enabled, any change in selections triggers reloading data automatically."
            )
            load_clicked = st.button("🚀 Load Data", use_container_width=True, key="sidebar_load_btn")
            clear_clicked = st.button("🧹 Clear cached data", use_container_width=True, key="sidebar_clear_cache_btn")

            if clear_clicked:
                try:
                    if hasattr(st, "cache_data"):
                        st.cache_data.clear()
                    if hasattr(st, "cache_resource"):
                        st.cache_resource.clear()
                    st.success("Cache cleared.")
                except Exception as e:
                    self._log_error(e, context="cache_clear")
                    st.warning("Cache clear attempted. If the issue persists, reload the app.")

            return {
                "selected_assets": selected_assets,
                "selected_benchmarks": selected_benchmarks,
                "start_date": start_date,
                "end_date": end_date,
                "auto_reload": auto_reload,
                "load_clicked": load_clicked,
            }

    def _load_sidebar_selection(self, sidebar_state: dict):
        """Load data based on sidebar state and populate session_state."""
        selected_assets = sidebar_state.get("selected_assets", [])
        selected_benchmarks = sidebar_state.get("selected_benchmarks", [])
        start_date = sidebar_state.get("start_date")
        end_date = sidebar_state.get("end_date")

        if not selected_assets:
            st.warning("Please select at least one asset from the sidebar.")
            st.session_state.data_loaded = False
            return

        # Normalize dates
        start_dt = datetime.combine(start_date, datetime.min.time())
        end_dt = datetime.combine(end_date, datetime.min.time())
        if end_dt <= start_dt:
            st.warning("End date must be after the start date.")
            st.session_state.data_loaded = False
            return

        # Hash selections to avoid unnecessary reloads
        selection_fingerprint = json.dumps(
            {
                "assets": selected_assets,
                "benchmarks": selected_benchmarks,
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
            },
            sort_keys=True,
        )
        selection_hash = hashlib.sha256(selection_fingerprint.encode("utf-8")).hexdigest()

        if st.session_state.get("last_selection_hash") == selection_hash and st.session_state.get("data_loaded", False):
            return

        st.session_state.last_selection_hash = selection_hash
        st.session_state.selected_assets = selected_assets
        st.session_state.selected_benchmarks = selected_benchmarks

        # Update analysis config dates (keep other defaults)
        cfg = st.session_state.get("analysis_config", AnalysisConfiguration(start_date=start_dt, end_date=end_dt))
        cfg.start_date = start_dt
        cfg.end_date = end_dt
        st.session_state.analysis_config = cfg

        with st.spinner("Loading market data..."):
            try:
                raw_assets = self.data_manager.fetch_multiple_assets(selected_assets, start_dt, end_dt, max_workers=4)
                raw_bench = self.data_manager.fetch_multiple_assets(selected_benchmarks, start_dt, end_dt, max_workers=3) if selected_benchmarks else {}

                asset_data = {}
                missing_assets = []
                for sym, df in (raw_assets or {}).items():
                    if df is None or df.empty:
                        missing_assets.append(sym)
                        continue
                    # Ensure Close exists
                    if "Close" not in df.columns and "Adj Close" in df.columns:
                        df["Close"] = df["Adj Close"]
                    df_feat = self.data_manager.calculate_technical_features(df)
                    asset_data[sym] = df_feat

                bench_data = {}
                missing_bench = []
                for sym, df in (raw_bench or {}).items():
                    if df is None or df.empty:
                        missing_bench.append(sym)
                        continue
                    if "Close" not in df.columns and "Adj Close" in df.columns:
                        df["Close"] = df["Adj Close"]
                    df_feat = self.data_manager.calculate_technical_features(df)
                    bench_data[sym] = df_feat

                if not asset_data:
                    st.session_state.data_loaded = False
                    st.error("No valid market data could be loaded for the selected assets. Try a wider date range or fewer tickers.")
                    if missing_assets:
                        st.info("Missing assets: " + ", ".join(missing_assets))
                    return

                # Build returns matrix (aligned)
                returns_df = pd.DataFrame({sym: df["Returns"] for sym, df in asset_data.items() if "Returns" in df.columns})
                returns_df = returns_df.dropna(how="all")

                bench_returns_df = pd.DataFrame({sym: df["Returns"] for sym, df in bench_data.items() if "Returns" in df.columns})
                bench_returns_df = bench_returns_df.dropna(how="all") if not bench_returns_df.empty else bench_returns_df

                st.session_state.asset_data = asset_data
                st.session_state.benchmark_data = bench_data
                st.session_state.returns_data = returns_df
                st.session_state.benchmark_returns_data = bench_returns_df
                st.session_state.data_loaded = True

                # Surface missing data as a soft warning
                if missing_assets:
                    st.sidebar.warning("Some assets returned no data: " + ", ".join(missing_assets))
                if missing_bench:
                    st.sidebar.warning("Some benchmarks returned no data: " + ", ".join(missing_bench))

                st.sidebar.success("Data loaded.")
            except Exception as e:
                self._log_error(e, context="data_load")
                st.session_state.data_loaded = False
                st.error(f"Data load failed: {e}")

    def _display_tracking_error(self, config: 'AnalysisConfiguration'):
        """Interactive Tracking Error analytics with institutional band zones.
        Robust implementation: always available even if earlier patch blocks were misplaced.
        """
        st.markdown("### 🎯 Tracking Error (Institutional Band Monitoring)")
        # --- Load returns
        to_df = getattr(self, "_to_returns_df", None)
        if callable(to_df):
            returns_df = to_df(st.session_state.get("returns_data", None))
            bench_df = to_df(st.session_state.get("benchmark_returns_data", None))
        else:
            returns_df = st.session_state.get("returns_data", None)
            bench_df = st.session_state.get("benchmark_returns_data", None)
            returns_df = returns_df.copy() if isinstance(returns_df, pd.DataFrame) else pd.DataFrame(returns_df) if isinstance(returns_df, dict) else pd.DataFrame()
            bench_df = bench_df.copy() if isinstance(bench_df, pd.DataFrame) else pd.DataFrame(bench_df) if isinstance(bench_df, dict) else pd.DataFrame()

        returns_df = returns_df.replace([np.inf, -np.inf], np.nan).dropna(axis=1, how="all")
        bench_df = bench_df.replace([np.inf, -np.inf], np.nan).dropna(axis=1, how="all")

        if returns_df.empty:
            st.info("Load data first to compute Tracking Error.")
            return
        if bench_df.empty:
            st.warning("No benchmark returns available. Please select at least one benchmark in the sidebar and reload data.")
            return

        key_ns = "te_tab__"

        # --- Controls
        c1, c2, c3, c4 = st.columns([1.2, 1.0, 1.0, 1.0])
        with c1:
            scope = st.selectbox(
                "Scope",
                ["Portfolio (Equal Weight)", "Single Asset"],
                index=0,
                key=f"{key_ns}scope",
                help="Compute tracking error for an equal-weight portfolio of selected assets or a single asset.",
            )
        with c2:
            window = st.selectbox(
                "Rolling window (days)",
                [20, 60, 126, 252],
                index=3,
                key=f"{key_ns}window",
            )
        with c3:
            green_thr = st.number_input(
                "Green threshold (TE)",
                min_value=0.0,
                max_value=1.0,
                value=float(st.session_state.get("te_green_thr", 0.04)),
                step=0.005,
                format="%.3f",
                key=f"{key_ns}green",
                help="Default institutional policy: TE < 4% = Green",
            )
        with c4:
            orange_thr = st.number_input(
                "Orange threshold (TE)",
                min_value=0.0,
                max_value=1.0,
                value=float(st.session_state.get("te_orange_thr", 0.08)),
                step=0.005,
                format="%.3f",
                key=f"{key_ns}orange",
                help="Default institutional policy: 4–8% = Orange, >8% = Red",
            )

        st.session_state["te_green_thr"] = float(green_thr)
        st.session_state["te_orange_thr"] = float(orange_thr)

        bcols = list(bench_df.columns)
        bench_col = st.selectbox(
            "Benchmark",
            bcols,
            index=0,
            key=f"{key_ns}bench",
            help="Benchmark series used for Tracking Error.",
        )

        # --- Build portfolio/asset series
        if scope.startswith("Portfolio"):
            assets = list(returns_df.columns)
            default_assets = assets[: min(6, len(assets))]
            sel_assets = st.multiselect(
                "Select assets for equal-weight portfolio",
                assets,
                default=default_assets,
                key=f"{key_ns}assets",
            )
            if not sel_assets:
                st.warning("Select at least 1 asset.")
                return
            port = returns_df[sel_assets].mean(axis=1)
            series_name = "EQW_Portfolio"
        else:
            assets = list(returns_df.columns)
            asset = st.selectbox(
                "Asset",
                assets,
                index=0,
                key=f"{key_ns}asset",
            )
            port = returns_df[asset]
            series_name = str(asset)

        bench = bench_df[bench_col]

        # --- Align / active
        idx = port.dropna().index.intersection(bench.dropna().index)
        if len(idx) < max(60, int(window)):
            st.warning("Not enough overlapping data points to compute robust Tracking Error.")
            return

        port = port.loc[idx].astype(float)
        bench = bench.loc[idx].astype(float)
        active = (port - bench).dropna()

        if active.empty:
            st.warning("Active return series is empty after alignment.")
            return

        # --- Tracking error series (rolling)
        te_roll = active.rolling(int(window)).std(ddof=1) * np.sqrt(252.0)
        te_roll.name = "TrackingError"
        te_last = float(te_roll.dropna().iloc[-1]) if te_roll.dropna().shape[0] else np.nan

        # --- KPI row
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Current TE (ann.)", f"{te_last:.2%}" if np.isfinite(te_last) else "N/A")
        k2.metric("Avg TE (ann.)", f"{float(te_roll.mean()):.2%}" if te_roll.dropna().shape[0] else "N/A")
        k3.metric("Max TE (ann.)", f"{float(te_roll.max()):.2%}" if te_roll.dropna().shape[0] else "N/A")
        k4.metric("Window", f"{int(window)}d")

        # --- Determine band range
        y_max = float(np.nanmax([te_roll.max(), orange_thr * 1.35, 0.12])) if te_roll.dropna().shape[0] else float(orange_thr * 1.35)
        y_max = max(y_max, orange_thr * 1.35, green_thr * 1.35, 0.05)

        # --- Plot with bands
        fig = go.Figure()

        # Bands (green/orange/red)
        x0 = te_roll.index.min()
        x1 = te_roll.index.max()
        fig.add_shape(type="rect", xref="x", yref="y", x0=x0, x1=x1, y0=0, y1=green_thr,
                      fillcolor="rgba(0,200,0,0.18)", line_width=0, layer="below")
        fig.add_shape(type="rect", xref="x", yref="y", x0=x0, x1=x1, y0=green_thr, y1=orange_thr,
                      fillcolor="rgba(255,165,0,0.18)", line_width=0, layer="below")
        fig.add_shape(type="rect", xref="x", yref="y", x0=x0, x1=x1, y0=orange_thr, y1=y_max,
                      fillcolor="rgba(255,0,0,0.16)", line_width=0, layer="below")

        fig.add_trace(go.Scatter(x=te_roll.index, y=te_roll.values, mode="lines", name="Rolling TE (ann.)"))
        if np.isfinite(te_last):
            fig.add_trace(go.Scatter(x=[te_roll.index[-1]], y=[te_last], mode="markers", name="Current", marker=dict(size=10)))

        fig.update_layout(
            title=f"Tracking Error — {series_name} vs {bench_col} (rolling {int(window)}d)",
            height=460,
            xaxis_title="Date",
            yaxis_title="Tracking Error (annualized)",
            margin=dict(l=10, r=10, t=60, b=10),
            legend_title="Series",
        )
        st.plotly_chart(fig, use_container_width=True, key=f"{key_ns}chart")

        # --- Weekly table (last TE per week)
        st.markdown("#### Weekly Tracking Error Snapshot")
        te_week = te_roll.resample("W-FRI").last().dropna()
        if te_week.empty:
            st.info("Weekly snapshot not available yet.")
        else:
            table = pd.DataFrame({
                "Week": te_week.index.strftime("%Y-%m-%d"),
                "TE": te_week.values,
            })
            def _band(v: float) -> str:
                if not np.isfinite(v):
                    return "N/A"
                if v < green_thr:
                    return "GREEN"
                if v < orange_thr:
                    return "ORANGE"
                return "RED"
            table["Band"] = [_band(v) for v in table["TE"]]
            table["TE"] = table["TE"].map(lambda x: f"{x:.2%}" if np.isfinite(x) else "N/A")
            st.dataframe(table.tail(30), use_container_width=True)

        with st.expander("Method Notes (Institutional)", expanded=False):
            st.markdown(
                """**Tracking Error (TE)** is the annualized standard deviation of **active returns** (Portfolio − Benchmark).\n\n"
                "- Rolling TE uses the selected window and annualizes by √252.\n"
                "- Band thresholds are configurable; typical policy: **<4% green**, **4–8% orange**, **>8% red**.\n"
                "- Portfolio scope here uses **equal weights** for the selected assets (manual optimizer weights are in Portfolio Lab tab)."""
            )

    def run(self):
        """Main app runner (Streamlit entry)."""
        try:
            self.display_header()

            sidebar_state = self._render_sidebar_controls()

            # Auto reload on changes (optional)
            if sidebar_state.get("auto_reload", False):
                # trigger load if fingerprint changed
                self._load_sidebar_selection(sidebar_state)
            # Explicit load button
            if sidebar_state.get("load_clicked", False):
                self._load_sidebar_selection(sidebar_state)

            # --- Ensure AnalysisConfiguration exists (used by all display tabs) ---


            cfg = st.session_state.get("analysis_config")


            if cfg is None or not isinstance(cfg, AnalysisConfiguration):


                cfg = AnalysisConfiguration()


                st.session_state["analysis_config"] = cfg


            if not st.session_state.get("data_loaded", False):
                self._display_welcome(cfg)
                return

            tab_labels = [
                "📊 Dashboard",
                "🧠 Advanced Analytics",
                "🧮 Risk Analytics",
                "📉 EWMA Ratio Signal",
                "📈 Portfolio",
                "🎯 Tracking Error",
                "β Rolling Beta",
                "📉 Relative VaR/CVaR/ES",
                "🧪 Stress Testing",
                "📑 Reporting",
                "⚙️ Settings",
                "🧰 Portfolio Lab (PyPortfolioOpt)",
            ]
            tabs = st.tabs(tab_labels)

            with tabs[0]:
                self._display_dashboard(cfg)
            with tabs[1]:
                self._display_advanced_analytics(cfg)
            with tabs[2]:
                self._display_risk_analytics(cfg)
            with tabs[3]:
                self._display_ewma_ratio_signal(cfg)
            with tabs[4]:
                self._display_portfolio(cfg)
            with tabs[5]:
                self._display_tracking_error(cfg)
            with tabs[6]:
                self._display_rolling_beta(cfg)
            with tabs[7]:
                self._display_relative_risk(cfg)
            with tabs[8]:
                self._display_stress_testing(cfg)
            with tabs[9]:
                self._display_reporting(cfg)
            with tabs[10]:
                self._display_settings(cfg)

            with tabs[11]:
                self._display_portfolio_lab(cfg)

        except Exception as e:
            self._log_error(e, context="run")
            st.error(f"🚨 Application Error: {e}")
            st.code(traceback.format_exc())

    def _display_welcome(self, config: Optional[AnalysisConfiguration] = None):
        """Display welcome screen (clean)."""

        st.markdown("### 🏛️ Welcome")
        st.write("Select assets and dates from the sidebar, then click **Load Data**.")

        with st.expander("🚀 Getting Started", expanded=True):
            st.markdown(
                """
- Select assets from the sidebar  
- Choose the date range  
- Click **Load Data**  
- Explore: **Dashboard**, **Portfolio**, **GARCH**, **Regimes**, **Analytics**, **Reports**
                """.strip()
            )

    def _display_dashboard(self, config: AnalysisConfiguration):
        """Display main dashboard"""
        st.markdown('<div class="section-header"><h2>📊 Market Dashboard</h2></div>', unsafe_allow_html=True)
        
        # Quick metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            returns_df = pd.DataFrame(st.session_state.returns_data).dropna()
            avg_return = returns_df.mean().mean() * 252 * 100 if not returns_df.empty else 0
            st.markdown(textwrap.dedent(f"""
            <div class="metric-card">
                <div class="metric-label">📈 Avg Annual Return</div>
                <div class="metric-value {'positive' if avg_return > 0 else 'negative'}">
                    {avg_return:.2f}%
                </div>
            </div>
            """), unsafe_allow_html=True)
        
        with col2:
            avg_vol = returns_df.std().mean() * np.sqrt(252) * 100 if not returns_df.empty else 0
            st.markdown(textwrap.dedent(f"""
            <div class="metric-card">
                <div class="metric-label">📉 Avg Volatility</div>
                <div class="metric-value">{avg_vol:.2f}%</div>
            </div>
            """), unsafe_allow_html=True)
        
        with col3:
            #  reporting removed per user request (including diagnostics & coverage).
            avg_skew = float(returns_df.skew().mean()) if not returns_df.empty else np.nan
            avg_skew_disp = "N/A" if (avg_skew is None or (isinstance(avg_skew, float) and np.isnan(avg_skew))) else f"{avg_skew:.3f}"
            st.markdown(textwrap.dedent(f"""
            <div class="metric-card">
                <div class="metric-label">📐 Avg Skewness</div>
                <div class="metric-value">{avg_skew_disp}</div>
            </div>
            """), unsafe_allow_html=True)

        with col4:
            # Widgets removed per user request.
            # Provide a stable informational KPI instead.
            n_assets = int(returns_df.shape[1]) if isinstance(returns_df, pd.DataFrame) and not returns_df.empty else 0
            n_obs = int(returns_df.shape[0]) if isinstance(returns_df, pd.DataFrame) and not returns_df.empty else 0
            st.markdown(textwrap.dedent(f"""
            <div class="metric-card">
                <div class="metric-label">📦 Assets / Obs</div>
                <div class="metric-value">{n_assets} / {n_obs}</div>
            </div>
            """), unsafe_allow_html=True)

# =============================================================================
# 🔧 SAFETY BINDERS — Ensure required dashboard methods exist (no AttributeErrors)
# =============================================================================

def _icd__to_returns_df_fallback(self, returns_data):
    """Robustly coerce session_state returns_data into a wide DataFrame."""
    import numpy as np
    import pandas as pd
    if returns_data is None:
        return pd.DataFrame()
    if isinstance(returns_data, pd.DataFrame):
        return returns_data.copy()
    if isinstance(returns_data, pd.Series):
        return returns_data.to_frame()
    if isinstance(returns_data, dict):
        cols = {}
        for k, v in returns_data.items():
            if v is None:
                continue
            if isinstance(v, pd.Series):
                cols[str(k)] = v
            elif isinstance(v, pd.DataFrame):
                if v.shape[1] >= 1:
                    cols[str(k)] = v.iloc[:, 0]
        if not cols:
            return pd.DataFrame()
        df = pd.concat(cols, axis=1)
        return df
    try:
        return pd.DataFrame(returns_data)
    except Exception:
        return pd.DataFrame()

def _icd_display_relative_risk_fallback(self, cfg):
    """
    Relative Risk Dashboard:
    - Relative VaR / CVaR(ES) (Historical) on active returns (Portfolio - Benchmark)
    - Tracking Error (annualized)
    - Interactive bands (green/orange/red) via thresholds
    This is a safe fallback to avoid AttributeError if the method was not merged into the class.
    """
    import numpy as np
    import pandas as pd
    import streamlit as st
    import plotly.graph_objects as go

    st.subheader("📊 Relative Risk (vs Benchmark) — Relative VaR / ES + Bands")

    # Returns universe
    if hasattr(self, "_to_returns_df"):
        returns_df = self._to_returns_df(st.session_state.get("returns_data", None))
    else:
        returns_df = _icd__to_returns_df_fallback(self, st.session_state.get("returns_data", None))

    returns_df = returns_df.replace([np.inf, -np.inf], np.nan).dropna(axis=1, how="all")

    if returns_df.empty:
        st.info("Relative risk cannot be computed: returns data is empty.")
        return

    # Benchmarks
    bench_dict = st.session_state.get("benchmark_returns", None)
    if not isinstance(bench_dict, dict):
        bench_dict = {}

    bench_options = ["(None)"] + list(bench_dict.keys())
    bmk_key = st.selectbox("Benchmark", options=bench_options, index=0, key="relrisk_bmk")
    bench = bench_dict.get(bmk_key) if bmk_key != "(None)" else None

    # Portfolio series
    port_series = st.session_state.get("portfolio_returns", None)
    if isinstance(port_series, pd.Series) and not port_series.dropna().empty:
        portfolio = port_series.dropna()
        st.caption("Using portfolio_returns from session_state.")
    else:
        asset = st.selectbox("Target (proxy portfolio): choose asset", options=list(returns_df.columns), index=0, key="relrisk_asset")
        portfolio = returns_df[asset].dropna()

    if bench is None or not isinstance(bench, pd.Series) or bench.dropna().empty:
        st.info("Select a benchmark to compute relative risk (active series).")
        return

    idx = portfolio.index.intersection(bench.dropna().index)
    if len(idx) < 60:
        st.warning("Insufficient overlap with benchmark (need ~60+ observations).")
        return

    active = (portfolio.loc[idx] - bench.loc[idx]).dropna()
    if active.empty:
        st.warning("Active series is empty after alignment.")
        return

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        alpha = st.select_slider("α (tail)", options=[0.10, 0.05, 0.025, 0.01], value=0.05, key="relrisk_alpha")
    with c2:
        horizon = st.selectbox("Horizon (days)", options=[1, 5, 10, 21], index=0, key="relrisk_h")
    with c3:
        window = st.selectbox("Rolling window", options=[63, 126, 252, 504], index=2, key="relrisk_win")
    with c4:
        green = st.number_input("Green ≤", value=0.02, step=0.005, format="%.4f", key="relrisk_green")
        orange = st.number_input("Orange ≤", value=0.04, step=0.005, format="%.4f", key="relrisk_orange")

    def _hist_var_es(series, a):
        losses = -series
        var_loss = float(np.quantile(losses, a))
        tail = losses[losses >= var_loss]
        es_loss = float(np.mean(tail)) if len(tail) else float(np.max(losses))
        # Convert back to return-space thresholds (negative for losses)
        return -var_loss, -es_loss

    roll_var = active.rolling(int(window)).apply(lambda s: _hist_var_es(pd.Series(s).dropna(), float(alpha))[0] if len(pd.Series(s).dropna()) else np.nan, raw=False)
    roll_es  = active.rolling(int(window)).apply(lambda s: _hist_var_es(pd.Series(s).dropna(), float(alpha))[1] if len(pd.Series(s).dropna()) else np.nan, raw=False)

    if int(horizon) > 1:
        scale = float(np.sqrt(int(horizon)))
        roll_var = roll_var * scale
        roll_es = roll_es * scale

    latest_var = float(roll_var.dropna().iloc[-1]) if not roll_var.dropna().empty else np.nan
    latest_es  = float(roll_es.dropna().iloc[-1]) if not roll_es.dropna().empty else np.nan
    te = float(active.std(ddof=1) * np.sqrt(252)) if active.dropna().shape[0] > 1 else np.nan

    k1, k2, k3 = st.columns(3)
    k1.metric("Tracking Error (ann.)", f"{te:.2%}" if np.isfinite(te) else "N/A")
    k2.metric(f"Active VaR (Hist, α={alpha}, h={horizon})", f"{latest_var:.2%}" if np.isfinite(latest_var) else "N/A")
    k3.metric(f"Active ES (Hist, α={alpha}, h={horizon})", f"{latest_es:.2%}" if np.isfinite(latest_es) else "N/A")

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=active.index, y=active.values, name="Active Return", mode="lines"))
    fig.add_trace(go.Scatter(x=roll_var.index, y=roll_var.values, name="Rolling Active VaR", mode="lines"))
    fig.add_trace(go.Scatter(x=roll_es.index, y=roll_es.values, name="Rolling Active ES", mode="lines"))

    g = float(green)
    o = float(orange)

    # Green band [-g, g]
    fig.add_shape(type="rect", xref="paper", yref="y", x0=0, x1=1, y0=-g, y1=g, opacity=0.15, line_width=0)
    # Orange band [-o, -g] and [g, o]
    fig.add_shape(type="rect", xref="paper", yref="y", x0=0, x1=1, y0=-o, y1=-g, opacity=0.12, line_width=0)
    fig.add_shape(type="rect", xref="paper", yref="y", x0=0, x1=1, y0=g, y1=o, opacity=0.12, line_width=0)
    # Reference lines
    fig.add_hline(y=o, line_width=1)
    fig.add_hline(y=-o, line_width=1)

    fig.update_layout(
        title="Active Return vs Rolling Relative VaR/ES (Bands via thresholds)",
        height=520,
        xaxis_title="Date",
        yaxis_title="Active Return / Risk",
        margin=dict(l=10, r=10, t=50, b=10)
    )
    st.plotly_chart(fig, use_container_width=True, key="relrisk_chart")


# Bind missing methods safely (no crashes)
try:
    InstitutionalCommoditiesDashboard  # noqa: F401
    if not hasattr(InstitutionalCommoditiesDashboard, "_to_returns_df"):
        InstitutionalCommoditiesDashboard._to_returns_df = _icd__to_returns_df_fallback
    if not hasattr(InstitutionalCommoditiesDashboard, "_display_relative_risk"):
        InstitutionalCommoditiesDashboard._display_relative_risk = _icd_display_relative_risk_fallback
except Exception:
    pass



# =============================================================================
# 🧠 ADDITIVE MODULE: Quantum Sovereign v14.0 (Integrated as a separate mode)
# - Integrated WITHOUT removing existing InstitutionalCommoditiesDashboard features.
# - Runs as an additional "Mode" in the same Streamlit app.
# - Heavy ML deps (xgboost / tensorflow) are optional; the UI will degrade gracefully if missing.
# =============================================================================

def run_quantum_sovereign_v14_terminal():
    """
    Quantum Sovereign v14.0 (Quantum Sovereign) — integrated mode
    Modules: Hybrid LSTM/RNN/XGBoost, ERC Portfolio Optimization, Black-Scholes Greeks,
    Macro Sensitivity, Automated Signals, and Performance Backtesting.
    """
    import os
    import math
    import warnings
    import json
    import hashlib
    import traceback
    import logging
    from datetime import datetime, timedelta
    from typing import Dict, Any, Optional, Tuple, List, Union, Callable
    from dataclasses import dataclass, field, asdict
    from functools import lru_cache, wraps
    from concurrent.futures import ThreadPoolExecutor, as_completed

    import numpy as np
    import pandas as pd
    import streamlit as st
    import yfinance as yf
    import plotly.graph_objects as go
    import plotly.express as px
    from plotly.subplots import make_subplots

    # Optional heavy deps — degrade gracefully instead of crashing Streamlit Cloud
    _XGB_OK = False
    _TF_OK = False
    _SKL_OK = False

    try:
        import xgboost as xgb
        _XGB_OK = True
    except Exception:
        xgb = None
        _XGB_OK = False

    try:
        from sklearn.preprocessing import MinMaxScaler
        _SKL_OK = True
    except Exception:
        MinMaxScaler = None
        _SKL_OK = False

    try:
        from tensorflow.keras.models import Sequential
        from tensorflow.keras.layers import LSTM, Dense, Dropout, SimpleRNN, BatchNormalization
        from tensorflow.keras.callbacks import EarlyStopping
        _TF_OK = True
    except Exception:
        Sequential = None
        LSTM = Dense = Dropout = SimpleRNN = BatchNormalization = None
        EarlyStopping = None
        _TF_OK = False

    from scipy import stats, optimize
    from scipy.stats import norm
    from scipy.optimize import minimize

    # =============================================================================
    # 1. ADVANCED METADATA & CONFIGURATION
    # =============================================================================

    @dataclass
    class AssetMetadata:
        symbol: str
        name: str
        category: str
        color: str
        risk_profile: str
        exchange: str

    # NOTE: Industrial Metals extended to include Aluminum (ALI=F) in addition to Copper (HG=F)
    ASSET_UNIVERSE = {
        "Energy": {
            "CL=F": AssetMetadata("CL=F", "Crude Oil WTI", "Energy", "#00d4ff", "High", "NYMEX"),
            "NG=F": AssetMetadata("NG=F", "Natural Gas", "Energy", "#4169E1", "High", "NYMEX"),
        },
        "Precious Metals": {
            "GC=F": AssetMetadata("GC=F", "Gold", "Metals", "#FFD700", "Low", "COMEX"),
            "SI=F": AssetMetadata("SI=F", "Silver", "Metals", "#C0C0C0", "Medium", "COMEX"),
        },
        "Industrial Metals": {
            "HG=F": AssetMetadata("HG=F", "Copper", "Metals", "#B87333", "Medium", "COMEX"),
            "ALI=F": AssetMetadata("ALI=F", "Aluminum", "Metals", "#A9A9A9", "Medium", "COMEX"),
        }
    }

    # =============================================================================
    # 2. DATA ACQUISITION & MANAGEMENT (Multi-threaded & Cached)
    # =============================================================================

    class InstitutionalDataManager:
        def __init__(self):
            self.session_data = {}

        @st.cache_data(ttl=3600)
        def get_data(self, tickers: List[str], period: str = "5y") -> pd.DataFrame:
            """High-performance multi-threaded data fetching."""
            try:
                data = yf.download(tickers, period=period, progress=False, threads=True)
                if isinstance(data.columns, pd.MultiIndex):
                    return data['Adj Close'].dropna()
                return data[['Adj Close']].rename(columns={'Adj Close': tickers[0]}).dropna()
            except Exception as e:
                st.error(f"Data Engine Critical Failure: {e}")
                return pd.DataFrame()

        def get_macro_data(self) -> pd.DataFrame:
            """Global Macro Overlay (DXY, 10Y Yields)."""
            macro = yf.download(["DX-Y.NYB", "^TNX"], period="5y", progress=False)['Adj Close']
            macro.columns = ["DXY", "US10Y"]
            return macro.pct_change(fill_method=None).replace([np.inf, -np.inf], np.nan).dropna()

    # =============================================================================
    # 3. HYBRID QUANTUM AI ENGINE (LSTM + ELMAN RNN + XGBOOST)
    # =============================================================================

    class QuantumAIEngine:
        def __init__(self, lookback: int = 60):
            self.lookback = lookback
            self.scalers = {}

        def _prepare_data(self, data: pd.Series):
            if not _SKL_OK or MinMaxScaler is None:
                raise RuntimeError("scikit-learn is required for MinMaxScaler.")
            scaler = MinMaxScaler(feature_range=(0, 1))
            scaled_data = scaler.fit_transform(data.values.reshape(-1, 1))
            X, y = [], []
            for i in range(self.lookback, len(scaled_data)):
                X.append(scaled_data[i-self.lookback:i, 0])
                y.append(scaled_data[i, 0])
            return np.array(X), np.array(y), scaled_data, scaler

        def build_lstm(self):
            if not _TF_OK or Sequential is None:
                raise RuntimeError("tensorflow is required for LSTM model.")
            model = Sequential([
                LSTM(100, return_sequences=True, input_shape=(self.lookback, 1)),
                BatchNormalization(),
                Dropout(0.3),
                LSTM(50, return_sequences=False),
                Dropout(0.3),
                Dense(25, activation='relu'),
                Dense(1)
            ])
            model.compile(optimizer='adam', loss='mse')
            return model

        def run_prediction(self, data: pd.Series, steps: int = 15) -> Dict[str, np.ndarray]:
            X, y, scaled_full, scaler = self._prepare_data(data)

            # 1. XGBoost Fit
            if not _XGB_OK or xgb is None:
                raise RuntimeError("xgboost is required for XGBoost model.")
            xgb_model = xgb.XGBRegressor(n_estimators=1000, max_depth=7, learning_rate=0.03, subsample=0.8)
            xgb_model.fit(X, y)

            # 2. LSTM Fit
            lstm_model = self.build_lstm()
            early_stop = EarlyStopping(monitor='loss', patience=5) if EarlyStopping is not None else None
            callbacks = [early_stop] if early_stop is not None else []
            lstm_model.fit(X.reshape(X.shape[0], X.shape[1], 1), y, epochs=20, batch_size=32, verbose=0, callbacks=callbacks)

            # Recursive Forecasting
            preds_xgb, preds_lstm = [], []
            curr_window_xgb = scaled_full[-self.lookback:].flatten()
            curr_window_lstm = scaled_full[-self.lookback:].reshape(1, self.lookback, 1)

            for _ in range(steps):
                # XGB Prediction
                p_xgb = xgb_model.predict(curr_window_xgb.reshape(1, -1))[0]
                preds_xgb.append(p_xgb)
                curr_window_xgb = np.append(curr_window_xgb[1:], p_xgb)

                # LSTM Prediction
                p_lstm = lstm_model.predict(curr_window_lstm, verbose=0)[0, 0]
                preds_lstm.append(p_lstm)
                curr_window_lstm = np.append(curr_window_lstm[:, 1:, :], [[[p_lstm]]], axis=1)

            return {
                "XGBoost": scaler.inverse_transform(np.array(preds_xgb).reshape(-1, 1)),
                "LSTM": scaler.inverse_transform(np.array(preds_lstm).reshape(-1, 1))
            }

    # =============================================================================
    # 4. SIGNAL & RISK INTELLIGENCE
    # =============================================================================

    class SignalIntelligence:
        @staticmethod
        def generate_trade_parameters(current_p, forecast_df, ann_vol):
            ensemble_forecast = forecast_df.mean(axis=1)
            target = ensemble_forecast.iloc[-1]
            expected_ret = (target - current_p) / current_p

            # Volatility adjusted Stop Loss (2.0x ATR Approximation)
            daily_vol = ann_vol / math.sqrt(252)
            sl_buffer = current_p * daily_vol * 2.0

            if expected_ret > 0.04:
                return {"Action": "STRONG BUY", "Color": "#00ff88", "SL": current_p - sl_buffer, "TP": current_p + (sl_buffer * 3)}
            elif expected_ret < -0.04:
                return {"Action": "STRONG SELL", "Color": "#ff3b3b", "SL": current_p + sl_buffer, "TP": current_p - (sl_buffer * 3)}
            return {"Action": "HOLD / NEUTRAL", "Color": "#888888", "SL": None, "TP": None}

    # =============================================================================
    # 5. DERIVATIVES & PORTFOLIO ENGINE
    # =============================================================================

    class QuantLibrary:
        @staticmethod
        def black_scholes_greeks(S, K, T, r, sigma, option_type="call"):
            d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
            d2 = d1 - sigma * np.sqrt(T)
            if option_type == "call":
                price = S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
                delta = norm.cdf(d1)
            else:
                price = K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)
                delta = norm.cdf(d1) - 1
            vega = S * norm.pdf(d1) * np.sqrt(T)
            return price, delta, vega

        @staticmethod
        def calculate_erc_weights(returns):
            cov = returns.cov().values * 252
            n = len(returns.columns)

            def objective(w):
                w = w.reshape(-1, 1)
                p_vol = np.sqrt(w.T @ cov @ w)
                rc = (w * (cov @ w)) / p_vol
                return np.sum((rc - p_vol/n)**2)

            res = minimize(objective, np.ones(n)/n, bounds=[(0,1)]*n, constraints={'type':'eq','fun':lambda x: np.sum(x)-1})
            return res.x

    # =============================================================================
    # 6. OMNI-TERMINAL APPLICATION INTERFACE
    # =============================================================================

    class SovereignTerminal:
        def __init__(self):
            # Keep the original intent but prevent Streamlit crash if already configured elsewhere
            try:
                st.set_page_config(page_title="Quantum Sovereign v14", layout="wide", initial_sidebar_state="expanded")
            except Exception:
                pass
            self.dm = InstitutionalDataManager()
            self.ai = QuantumAIEngine()
            self.quant = QuantLibrary()

        def apply_custom_css(self):
            st.markdown("""
            <style>
                .stApp { background-color: #0b0d11; }
                .metric-container { background: #151921; padding: 20px; border-radius: 12px; border: 1px solid #2d343f; }
                .header-text { color: #00d4ff; font-family: 'Inter', sans-serif; font-weight: 800; }
            </style>
            """, unsafe_allow_html=True)

        def _dep_warnings(self):
            missing = []
            if not _XGB_OK:
                missing.append("xgboost")
            if not _TF_OK:
                missing.append("tensorflow")
            if not _SKL_OK:
                missing.append("scikit-learn")
            if missing:
                st.warning("Quantum AI modules require extra packages not found in this environment: " + ", ".join(missing))
                st.code(
                    "requirements.txt suggestions:\n"
                    "xgboost\n"
                    "scikit-learn\n"
                    "tensorflow\n"
                )

        def run(self):
            self.apply_custom_css()
            st.sidebar.markdown("<h1 class='header-text'>🏛️ Quantum Sovereign</h1>", unsafe_allow_html=True)

            self._dep_warnings()

            # Sidebar Universe Selection
            category = st.sidebar.selectbox("Market Segment", list(ASSET_UNIVERSE.keys()), key="qs_category")
            selected_tickers = st.sidebar.multiselect(
                "Active Assets",
                list(ASSET_UNIVERSE[category].keys()),
                default=list(ASSET_UNIVERSE[category].keys())[:2],
                key="qs_assets"
            )

            period = st.sidebar.selectbox("History window", ["1y", "2y", "5y", "10y", "max"], index=2, key="qs_period")

            if st.sidebar.button("INITIALIZE TERMINAL EXECUTION", key="qs_init"):
                with st.spinner("Processing Quantum Models..."):
                    # Data Ingestion
                    price_data = self.dm.get_data(selected_tickers, period=period)
                    if price_data is None or price_data.empty:
                        st.error("No price data returned.")
                        return

                    returns = price_data.pct_change(fill_method=None).replace([np.inf, -np.inf], np.nan).dropna(how="all")
                    macro_data = self.dm.get_macro_data()

                    # Main Dashboard Layout
                    t1, t2, t3, t4, t5 = st.tabs(["📡 Signals", "🧠 AI Forecast", "🌍 Macro & Correlation", "🧮 Portfolio Lab", "🎫 Options"])

                    # Cache forecasts per ticker to avoid undefined variables between tabs
                    if "qs_forecasts" not in st.session_state:
                        st.session_state["qs_forecasts"] = {}

                    with t1:
                        st.markdown("### 📡 Automated Trade Signals (Ensemble Intelligence)")
                        for ticker in selected_tickers:
                            if ticker not in price_data.columns:
                                continue
                            curr_p = float(price_data[ticker].iloc[-1])
                            vol = float(returns[ticker].std(ddof=1) * np.sqrt(252)) if ticker in returns.columns else np.nan

                            forecast_dict = None
                            f_df = None
                            if _XGB_OK and _TF_OK and _SKL_OK:
                                try:
                                    forecast_dict = self.ai.run_prediction(price_data[ticker])
                                    f_df = pd.DataFrame(forecast_dict)
                                    st.session_state["qs_forecasts"][ticker] = f_df
                                except Exception as e:
                                    st.warning(f"AI forecast failed for {ticker}: {e}")
                                    f_df = None
                            else:
                                f_df = None

                            signal = {"Action": "HOLD / NEUTRAL", "Color": "#888888", "SL": None, "TP": None}
                            if f_df is not None and not f_df.empty and np.isfinite(vol):
                                signal = SignalIntelligence.generate_trade_parameters(curr_p, f_df, vol)

                            # UI Card
                            col_sig, col_chart = st.columns([1, 2])
                            with col_sig:
                                st.markdown(f"""
                                <div class='metric-container'>
                                    <h3 style='color:#8892b0'>{ticker}</h3>
                                    <h2 style='color:{signal['Color']}'>{signal['Action']}</h2>
                                    <p>Entry: ${curr_p:.2f}</p>
                                    {f"<p style='color:#00ff88'>TP: ${signal['TP']:.2f}</p><p style='color:#ff3b3b'>SL: ${signal['SL']:.2f}</p>" if signal['SL'] else ""}
                                </div>
                                """, unsafe_allow_html=True)

                            with col_chart:
                                fig = go.Figure()
                                fig.add_trace(go.Scatter(y=price_data[ticker].values[-40:], name="Historical", line=dict(color="#5161f1")))
                                if f_df is not None and not f_df.empty:
                                    fig.add_trace(go.Scatter(
                                        x=np.arange(40, 40 + len(f_df)),
                                        y=f_df.mean(axis=1).values,
                                        name="AI Ensemble",
                                        line=dict(dash='dash', color=signal['Color'])
                                    ))
                                fig.update_layout(template="plotly_dark", height=250, margin=dict(l=0, r=0, t=20, b=0))
                                st.plotly_chart(fig, use_container_width=True, key=f"qs_sig_chart_{ticker}")

                    with t2:
                        st.markdown("### 🧠 Quantum AI Decomposition")
                        st.write("Comparison of LSTM (Deep Learning) vs XGBoost (Gradient Boosting) Paths")

                        if not (_XGB_OK and _TF_OK and _SKL_OK):
                            st.info("Install xgboost + tensorflow + scikit-learn to enable AI forecast comparison charts.")
                        else:
                            pick = st.selectbox("Select asset", selected_tickers, index=0, key="qs_ai_pick")
                            f_df = st.session_state.get("qs_forecasts", {}).get(pick, None)
                            if f_df is None:
                                try:
                                    forecast_dict = self.ai.run_prediction(price_data[pick])
                                    f_df = pd.DataFrame(forecast_dict)
                                    st.session_state["qs_forecasts"][pick] = f_df
                                except Exception as e:
                                    st.error(f"Forecast failed: {e}")
                                    f_df = None
                            if f_df is not None:
                                st.line_chart(f_df)

                    with t3:
                        st.markdown("### 🌍 Macro Sensitivity & Cross-Asset Beta")
                        combined = pd.concat([returns, macro_data], axis=1).dropna()
                        if combined.empty:
                            st.info("Not enough overlapping macro + asset returns.")
                        else:
                            corr_matrix = combined.corr(min_periods=60)
                            try:
                                st.plotly_chart(px.imshow(corr_matrix, text_auto=".2f", color_continuous_scale="RdBu_r", template="plotly_dark"),
                                                use_container_width=True, key="qs_macro_corr")
                            except Exception:
                                st.plotly_chart(px.imshow(corr_matrix, color_continuous_scale="RdBu_r", template="plotly_dark"),
                                                use_container_width=True, key="qs_macro_corr2")

                    with t4:
                        st.markdown("### 🧮 Institutional Portfolio Allocation")
                        if returns.shape[1] < 2:
                            st.info("Need at least 2 assets to compute ERC weights.")
                        else:
                            weights = self.quant.calculate_erc_weights(returns)
                            weight_df = pd.DataFrame({"Asset": list(returns.columns), "Weight": weights})
                            st.plotly_chart(px.pie(weight_df, values='Weight', names='Asset', hole=0.5,
                                                   title="Equal Risk Contribution (ERC) Allocation", template="plotly_dark"),
                                            use_container_width=True, key="qs_erc_pie")
                            st.dataframe(weight_df.set_index("Asset"), use_container_width=True)

                    with t5:
                        st.markdown("### 🎫 Options Hub (Derivatives Pricing)")
                        selected_opt = st.selectbox("Select Asset for Pricing", selected_tickers, key="qs_opt_asset")
                        S = float(price_data[selected_opt].iloc[-1])
                        K = st.number_input("Strike Price", value=float(S), key="qs_opt_strike")
                        vol_opt = float(returns[selected_opt].std(ddof=1) * np.sqrt(252)) if selected_opt in returns.columns else np.nan
                        T = st.number_input("Time to maturity (years)", value=0.10, step=0.01, key="qs_opt_T")
                        r = st.number_input("Risk-free rate", value=0.04, step=0.005, key="qs_opt_r")
                        opt_type = st.selectbox("Option Type", ["call", "put"], index=0, key="qs_opt_type")

                        if not np.isfinite(vol_opt) or vol_opt <= 0:
                            st.info("Volatility cannot be computed for options pricing (insufficient returns).")
                        else:
                            p, d, v = self.quant.black_scholes_greeks(S, K, float(T), float(r), vol_opt, option_type=opt_type)
                            st.metric(f"Option Price ({opt_type.upper()})", f"${p:.2f}")
                            st.write(f"Delta: {d:.3f} | Vega: {v:.2f}")

            else:
                st.info("Use the sidebar to select assets and click **INITIALIZE TERMINAL EXECUTION**.")

    # Run terminal
    SovereignTerminal().run()


# =============================================================================
# 🧭 APPLICATION ROUTER — Mode selector (Institutional v6.x + Quantum Sovereign v14)
# =============================================================================



# =============================================================================
# 🧬 MERGED MODULE: Scientific Commodities Platform v7.2 ULTRA (fixed)
# =============================================================================

"""
🏛️ Institutional Commodities Analytics Platform v7.2 (Ultra)
Enhanced Scientific Analytics • Robust Correlations (incl. Ledoit–Wolf) • Professional Risk Metrics
Institutional-Grade Computational Finance Platform (Streamlit Single-File Edition)

Key Upgrades (v7.2)
- ✅ Correct correlation matrix + PSD-safe nearest-correlation fix (Higham-style)
- ✅ Optional Ledoit–Wolf shrinkage correlation (scikit-learn)
- ✅ New Institutional Signal tab:
      (EWMA 22D Vol) / (EWMA 33D Vol + EWMA 99D Vol)
      + Bollinger Bands + Green/Orange/Red risk bands
- ✅ Real benchmark-based Treynor + Information Ratio (no random benchmark)
- ✅ Hard crash fixes: `import scipy` + Higham DataFrame-safe implementation
- ✅ NEW (added without removing core platform features):
      • Interactive Tracking Error tab with green/orange/red band zones
      • Rolling Beta tab
      • Relative VaR / CVaR / ES vs benchmark chart with band zones
"""

# =============================================================================
# IMPORTS (DO NOT MOVE st.set_page_config BELOW IMPORTS THAT REQUIRE st)
# =============================================================================
import os
import math
import json
import time
import warnings
import traceback
import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Tuple, List, Union

import numpy as np
import pandas as pd
import streamlit as st
import yfinance as yf

import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

# Scientific stack
import scipy  # ✅ REQUIRED because we reference scipy.__version__
from scipy import stats

# Optional visualization extras
try:
    import seaborn as sns  # not required, but kept for compatibility in user environments
except Exception:
    sns = None

# =============================================================================
# STREAMLIT PAGE CONFIG (MUST BE FIRST STREAMLIT COMMAND)
# =============================================================================
try:
    st.set_page_config(
        page_title="Institutional Commodities Analytics Platform v7.2",
        page_icon="🏛️",
        layout="wide",
        initial_sidebar_state="expanded"
    )
except Exception:
    # set_page_config can only be called once; ignore if already set
    pass

# =============================================================================
# GLOBAL SETTINGS
# =============================================================================
warnings.filterwarnings("ignore")
os.environ["NUMEXPR_MAX_THREADS"] = os.environ.get("NUMEXPR_MAX_THREADS", "8")
os.environ["OMP_NUM_THREADS"] = os.environ.get("OMP_NUM_THREADS", "4")
os.environ["MKL_NUM_THREADS"] = os.environ.get("MKL_NUM_THREADS", "4")

# =============================================================================
# STYLE (INSTITUTIONAL UI)
# =============================================================================
def _inject_css() -> None:
    css = """
    <style>
    :root{
        --bg:#0b1220;
        --card:#111a2e;
        --card2:#0f172a;
        --stroke:rgba(255,255,255,0.08);
        --text:#e5e7eb;
        --muted:#9ca3af;
        --accent:#60a5fa;
        --green:#22c55e;
        --orange:#f59e0b;
        --red:#ef4444;
        --purple:#a78bfa;
        --cyan:#22d3ee;
    }
    .block-container{padding-top:1.2rem;}
    .institutional-hero{
        border:1px solid var(--stroke);
        background: linear-gradient(135deg, rgba(96,165,250,0.18), rgba(167,139,250,0.12));
        padding: 1.2rem 1.2rem;
        border-radius: 16px;
        margin-bottom: 1rem;
    }
    .institutional-hero h1{
        margin:0;
        color: var(--text);
        font-size: 1.8rem;
        letter-spacing: 0.2px;
    }
    .institutional-hero p{
        margin:.3rem 0 0 0;
        color: var(--muted);
        font-size: .95rem;
    }
    .section-header{
        display:flex;
        align-items:flex-end;
        justify-content:space-between;
        gap:1rem;
        border:1px solid var(--stroke);
        background: rgba(255,255,255,0.02);
        padding: .8rem 1rem;
        border-radius: 14px;
        margin: 0.3rem 0 0.9rem 0;
    }
    .section-header h2{
        margin:0;
        color: var(--text);
        font-size:1.2rem;
    }
    .section-actions{display:flex; gap:.4rem; flex-wrap:wrap; justify-content:flex-end;}
    .scientific-badge{
        display:inline-flex;
        align-items:center;
        padding:.25rem .55rem;
        border-radius:999px;
        border:1px solid var(--stroke);
        font-size:.78rem;
        color: var(--text);
        background: rgba(255,255,255,0.03);
        white-space:nowrap;
    }
    .scientific-badge.info{border-color:rgba(96,165,250,0.35); background:rgba(96,165,250,0.12);}
    .scientific-badge.low-risk{border-color:rgba(34,197,94,0.35); background:rgba(34,197,94,0.12);}
    .scientific-badge.medium-risk{border-color:rgba(245,158,11,0.35); background:rgba(245,158,11,0.12);}
    .scientific-badge.high-risk{border-color:rgba(239,68,68,0.35); background:rgba(239,68,68,0.12);}
    .institutional-card{
        border:1px solid var(--stroke);
        background: rgba(17,26,46,0.55);
        padding: 1rem;
        border-radius: 16px;
        margin-bottom: 0.8rem;
    }
    .metric-title{color:var(--muted); font-size:.85rem; margin-bottom:.2rem;}
    .metric-value{color:var(--text); font-size:1.6rem; font-weight:700;}
    .subtle{color:var(--muted);}
    hr{border-color: rgba(255,255,255,0.08)!important;}
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)

_inject_css()

# =============================================================================
# DEPENDENCY MANAGER (OPTIONAL MODULES)
# =============================================================================
class ScientificDependencyManager:
    def __init__(self):
        self._cache: Dict[str, bool] = {}

    def is_available(self, name: str) -> bool:
        if name in self._cache:
            return self._cache[name]
        try:
            __import__(name)
            self._cache[name] = True
        except Exception:
            self._cache[name] = False
        return self._cache[name]

sci_dep_manager = ScientificDependencyManager()

# =============================================================================
# HELPERS
# =============================================================================
def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        if isinstance(x, (float, int, np.floating, np.integer)):
            if np.isnan(x):
                return default
            return float(x)
        val = float(x)
        if np.isnan(val):
            return default
        return val
    except Exception:
        return default

def _annualize_mean(daily_mean: float, trading_days: int = 252) -> float:
    return daily_mean * trading_days

def _annualize_vol(daily_std: float, trading_days: int = 252) -> float:
    return daily_std * math.sqrt(trading_days)

def _max_drawdown_from_returns(returns: pd.Series) -> float:
    if returns is None or returns.dropna().empty:
        return 0.0
    eq = (1.0 + returns.fillna(0.0)).cumprod()
    peak = eq.cummax()
    dd = (eq / peak) - 1.0
    return float(dd.min())

def _ewma_vol_annualized(returns: pd.Series, span: int, trading_days: int = 252) -> pd.Series:
    r = returns.astype(float)
    ewm_var = (r ** 2).ewm(span=span, adjust=False, min_periods=max(10, int(span * 0.7))).mean()
    return np.sqrt(ewm_var) * np.sqrt(trading_days)

# =============================================================================
# CONFIGURATION
# =============================================================================
@dataclass
class ScientificAnalysisConfiguration:
    lookback_years: int = 5
    interval: str = "1d"
    annual_trading_days: int = 252
    risk_free_rate: float = 0.03  # annual

    correlation_method: str = field(default="pearson")
    ewma_lambda: float = 0.94
    ensure_psd_corr: bool = True


    # Correlation data hygiene
    correlation_listwise: bool = True  # True: drop any-NA rows (recommended for stable, symmetric matrices)
    min_corr_obs: int = 60            # minimum observations required to compute a stable correlation matrix

    var_confidence: float = 0.95
    var_horizon_days: int = 1
    use_student_t_parametric: bool = True

    rolling_beta_window: int = 63
    tracking_error_window: int = 63

    vol_ratio_green_max: float = 0.35
    vol_ratio_orange_max: float = 0.55

    te_green_max: float = 0.04
    te_orange_max: float = 0.08

    relvar_green_max: float = 1.0
    relvar_orange_max: float = 2.0

# =============================================================================
# DATA MANAGER
# =============================================================================
class ScientificDataManager:
    def __init__(self, cfg: ScientificAnalysisConfiguration):
        self.cfg = cfg

    @st.cache_data(show_spinner=False)
    def fetch_prices(_self, tickers: Tuple[str, ...], start: str, end: str, interval: str) -> pd.DataFrame:
        try:
            df = yf.download(
                list(tickers),
                start=start,
                end=end,
                interval=interval,
                auto_adjust=True,
                progress=False,
                threads=True
            )
            if df is None or df.empty:
                return pd.DataFrame()
            if isinstance(df.columns, pd.MultiIndex):
                if "Close" in df.columns.get_level_values(0):
                    close = df["Close"].copy()
                else:
                    close = df.xs("Close", axis=1, level=0, drop_level=False)
                    if isinstance(close, pd.DataFrame) and close.shape[1] > 0:
                        close.columns = close.columns.get_level_values(-1)
                close = close.sort_index()
                return close
            if "Close" in df.columns:
                out = df[["Close"]].copy()
                out.columns = [tickers[0]]
                return out
            return df
        except Exception:
            return pd.DataFrame()

    def compute_returns(self, prices: pd.DataFrame) -> pd.DataFrame:
        """Compute returns with Cloud-safe and research-grade hygiene.

        Key improvements vs vanilla pct_change():
        - Explicitly disables pct_change forward-filling (fill_method=None). Forward-fill artifacts are a common
          root-cause of 'wrong' correlations and spurious low-vol periods.
        - Sorts index, drops duplicated timestamps, enforces float dtype, and removes infinities.
        """
        if prices is None or prices.empty:
            return pd.DataFrame()

        # Defensive copy + stable index
        px = prices.copy()
        try:
            px = px[~px.index.duplicated(keep="last")]
        except Exception:
            pass
        try:
            px = px.sort_index()
        except Exception:
            pass

        # Numeric coercion (silent -> NaN) for safety
        for c in px.columns:
            px[c] = pd.to_numeric(px[c], errors="coerce")

        # Critical: disable implicit forward-fill when computing pct_change
        rets = px.pct_change(fill_method=None).replace([np.inf, -np.inf], np.nan)

        return rets

    def calculate_scientific_features(self, prices: pd.Series) -> pd.DataFrame:
        if prices is None or prices.dropna().empty:
            return pd.DataFrame()

        df = pd.DataFrame(index=prices.index)
        df["Price"] = prices.astype(float)
        df["Returns"] = df["Price"].pct_change()

        try:
            if df["Returns"].notna().sum() >= 120:
                r = df["Returns"].copy()

                df["EWMA_Vol_22"] = _ewma_vol_annualized(r, 22, self.cfg.annual_trading_days) * 100.0
                df["EWMA_Vol_33"] = _ewma_vol_annualized(r, 33, self.cfg.annual_trading_days) * 100.0
                df["EWMA_Vol_99"] = _ewma_vol_annualized(r, 99, self.cfg.annual_trading_days) * 100.0

                denom = (df["EWMA_Vol_33"] + df["EWMA_Vol_99"]) + 1e-12
                df["EWMA_Vol_Ratio_22_over_33_99"] = df["EWMA_Vol_22"] / denom

                ratio = df["EWMA_Vol_Ratio_22_over_33_99"]
                bb_n = 20
                if ratio.notna().sum() >= bb_n:
                    mid = ratio.rolling(bb_n, min_periods=int(bb_n * 0.8)).mean()
                    sd = ratio.rolling(bb_n, min_periods=int(bb_n * 0.8)).std()
                    df["EWMA_Ratio_BB_Mid"] = mid
                    df["EWMA_Ratio_BB_Upper"] = mid + 2.0 * sd
                    df["EWMA_Ratio_BB_Lower"] = mid - 2.0 * sd
        except Exception:
            pass

        return df

# =============================================================================
# CORRELATION ENGINE
# =============================================================================
class ScientificCorrelationEngine:
    def __init__(self, cfg: ScientificAnalysisConfiguration):
        self.cfg = cfg

    def _calculate_ewma_cov(self, data: pd.DataFrame, lam: float) -> np.ndarray:
        X = data.dropna().values
        if X.shape[0] < 5:
            return np.cov(data.dropna().values, rowvar=False)
        n = X.shape[0]
        w = np.array([(1 - lam) * (lam ** (n - 1 - i)) for i in range(n)], dtype=float)
        w = w / (w.sum() + 1e-12)
        mean = np.average(X, axis=0, weights=w)
        Xc = X - mean
        cov = (Xc.T * w) @ Xc
        return cov

    def _cov_to_corr(self, cov: np.ndarray, cols: List[str]) -> pd.DataFrame:
        d = np.sqrt(np.diag(cov))
        denom = np.outer(d, d) + 1e-12
        corr = cov / denom
        corr = np.clip(corr, -0.9999, 0.9999)
        np.fill_diagonal(corr, 1.0)
        return pd.DataFrame(corr, index=cols, columns=cols)

    def _calculate_ledoit_wolf_correlation(self, data: pd.DataFrame) -> pd.DataFrame:
        if not sci_dep_manager.is_available("sklearn"):
            st.warning("Ledoit-Wolf requires scikit-learn. Falling back to Pearson correlation.")
            return data.corr(method="pearson")
        try:
            from sklearn.covariance import LedoitWolf
            X = data.dropna().values
            if X.shape[0] < 30:
                return data.corr(method="pearson")
            lw = LedoitWolf().fit(X)
            cov = lw.covariance_
            return self._cov_to_corr(cov, list(data.columns))
        except Exception as e:
            st.warning(f"Ledoit-Wolf correlation failed: {e}. Falling back to Pearson.")
            return data.corr(method="pearson")

    def _calculate_correlation_method(self, data: pd.DataFrame, method: str) -> pd.DataFrame:
        m = (method or "pearson").lower().strip()
        if m in ("pearson", "spearman", "kendall"):
            return data.corr(method=m)
        if m == "ewma":
            cov = self._calculate_ewma_cov(data, self.cfg.ewma_lambda)
            return self._cov_to_corr(cov, list(data.columns))
        if m == "ledoit_wolf":
            return self._calculate_ledoit_wolf_correlation(data)
        return data.corr(method="pearson")

    def _higham_nearest_correlation(self, A: Union[np.ndarray, pd.DataFrame], max_iter: int = 100) -> pd.DataFrame:
        if isinstance(A, pd.DataFrame):
            idx = A.index
            cols = A.columns
            X = A.values.copy()
        else:
            X = np.array(A, dtype=float, copy=True)
            idx = list(range(X.shape[0]))
            cols = list(range(X.shape[1]))

        X = (X + X.T) / 2.0
        for _ in range(max_iter):
            Y = X.copy()
            np.fill_diagonal(Y, 1.0)
            try:
                eigvals, eigvecs = np.linalg.eigh(Y)
                eigvals = np.maximum(eigvals, 0.0)
                X_new = eigvecs @ np.diag(eigvals) @ eigvecs.T
            except Exception:
                break
            if np.linalg.norm(X_new - X, "fro") < 1e-10:
                X = X_new
                break
            X = X_new

        np.fill_diagonal(X, 1.0)
        X = np.clip(X, -0.9999, 0.9999)
        np.fill_diagonal(X, 1.0)
        return pd.DataFrame(X, index=idx, columns=cols)

    def ensure_psd(self, corr: pd.DataFrame) -> pd.DataFrame:
        if corr is None or corr.empty:
            return pd.DataFrame()
        corr = corr.copy()
        corr = (corr + corr.T) / 2.0
        np.fill_diagonal(corr.values, 1.0)
        try:
            eig = np.linalg.eigvalsh(corr.values)
            if np.min(eig) < -1e-10:
                corr = self._higham_nearest_correlation(corr)
        except Exception:
            corr = self._higham_nearest_correlation(corr)
        corr = corr.clip(-0.9999, 0.9999)
        np.fill_diagonal(corr.values, 1.0)
        return corr

    def compute_correlation(self, returns: pd.DataFrame, method: str) -> pd.DataFrame:
        """Compute correlation matrix with strong numerical and data hygiene.

        Why this exists:
        - Pandas corr() defaults to pairwise deletion. That can yield *inconsistent effective sample sizes*
          across pairs, which may look 'wrong' and can also introduce numerical issues.
        - We therefore offer listwise (complete-case) correlation by default via cfg.correlation_listwise.
        """
        if returns is None or returns.empty:
            return pd.DataFrame()

        # Clean + enforce numeric
        data = returns.copy()
        data = data.replace([np.inf, -np.inf], np.nan)
        data = data.dropna(how="all").dropna(axis=1, how="all")

        if data.shape[1] < 2:
            return pd.DataFrame()

        # Listwise deletion (recommended) vs pairwise deletion (pandas default)
        if getattr(self.cfg, "correlation_listwise", True):
            data = data.dropna(how="any")

        # Minimum obs gate (prevents tiny overlaps causing unstable correlations)
        min_obs = int(getattr(self.cfg, "min_corr_obs", 60))
        if data.shape[0] < max(10, min_obs):
            return pd.DataFrame()

        corr = self._calculate_correlation_method(data.astype(float), method)
        corr = corr.replace([np.inf, -np.inf], np.nan).fillna(0.0)
        np.fill_diagonal(corr.values, 1.0)
        if self.cfg.ensure_psd_corr:
            corr = self.ensure_psd(corr)
        return corr

# =============================================================================
# ANALYTICS ENGINE
# =============================================================================
class ScientificAnalyticsEngine:
    def __init__(self, cfg: ScientificAnalysisConfiguration):
        self.cfg = cfg

    def calculate_scientific_risk_metrics(self, returns: pd.Series, benchmark_returns: Optional[pd.Series] = None) -> Dict[str, Any]:
        if returns is None or returns.dropna().empty:
            return {}
        r = returns.dropna().astype(float).replace([np.inf, -np.inf], np.nan).dropna()
        if r.empty:
            return {}

        ann_ret = _annualize_mean(r.mean(), self.cfg.annual_trading_days)
        ann_vol = _annualize_vol(r.std(ddof=1), self.cfg.annual_trading_days)
        sharpe = (ann_ret - self.cfg.risk_free_rate) / (ann_vol + 1e-12)

        downside = r[r < 0.0]
        downside_vol = _annualize_vol(downside.std(ddof=1), self.cfg.annual_trading_days) if len(downside) > 5 else 0.0
        sortino = (ann_ret - self.cfg.risk_free_rate) / (downside_vol + 1e-12) if downside_vol > 0 else 0.0

        mdd = _max_drawdown_from_returns(r)

        var_h, cvar_h = self._historical_var_cvar(r, self.cfg.var_confidence, self.cfg.var_horizon_days)
        var_p, cvar_p = self._parametric_var_cvar(r, self.cfg.var_confidence, self.cfg.var_horizon_days, use_t=self.cfg.use_student_t_parametric)

        beta, alpha = self._capm_beta_alpha(r, benchmark_returns)
        treynor = self._calculate_treynor_ratio(r, benchmark_returns)
        info_ratio = self._calculate_information_ratio(r, benchmark_returns)
        tracking_error = self._tracking_error(r, benchmark_returns)

        return {
            "Ann_Return": float(ann_ret),
            "Ann_Vol": float(ann_vol),
            "Sharpe": float(sharpe),
            "Sortino": float(sortino),
            "Max_Drawdown": float(mdd),
            "Hist_VaR": float(var_h),
            "Hist_CVaR_ES": float(cvar_h),
            "Param_VaR": float(var_p),
            "Param_CVaR": float(cvar_p),
            "Beta": float(beta),
            "Alpha": float(alpha),
            "Treynor_Ratio": float(treynor),
            "Information_Ratio": float(info_ratio),
            "Tracking_Error": float(tracking_error),
        }

    def _historical_var_cvar(self, r: pd.Series, confidence: float, horizon: int) -> Tuple[float, float]:
        # Robust cleaning to avoid NaNs (e.g., ddof issues, infs, tiny samples)
        try:
            rr = pd.to_numeric(r, errors="coerce")
        except Exception:
            rr = r.copy()
        rr = rr.replace([np.inf, -np.inf], np.nan).dropna()
        if rr is None or rr.empty or len(rr) < 2:
            return 0.0, 0.0

        h = math.sqrt(max(1, int(horizon)))
        scaled = rr * h  # volatility-dominated horizon scaling
        q = float(np.quantile(scaled.values, 1 - float(confidence)))
        var = -q
        tail = scaled[scaled <= q]
        cvar = -float(tail.mean()) if len(tail) > 0 else float(var)

        # Guard
        if not np.isfinite(var):
            var = 0.0
        if not np.isfinite(cvar):
            cvar = 0.0
        return float(max(0.0, var)), float(max(0.0, cvar))


    def _parametric_var_cvar(self, r: pd.Series, confidence: float, horizon: int, use_t: bool = True) -> Tuple[float, float]:
        # Robust cleaning: avoids NaN std() when sample is too small or non-numeric
        try:
            rr = pd.to_numeric(r, errors="coerce")
        except Exception:
            rr = r.copy()
        rr = rr.replace([np.inf, -np.inf], np.nan).dropna()
        if rr is None or rr.empty or len(rr) < 2:
            return 0.0, 0.0

        n = int(len(rr))
        mu = float(rr.mean()) if n > 0 else 0.0
        sigma = float(rr.std(ddof=1)) if n >= 2 else 0.0
        if (not np.isfinite(sigma)) and n >= 2:
            sigma = float(rr.std(ddof=0))
        if (not np.isfinite(mu)):
            mu = 0.0
        if (not np.isfinite(sigma)) or sigma < 1e-12:
            return 0.0, 0.0

        days = max(1, int(horizon))
        mu_h = mu * float(days)
        sigma_h = sigma * math.sqrt(float(days))

        alpha = 1.0 - float(confidence)

        # Optional Student-t (fit only when enough data)
        if use_t and n >= 80:
            try:
                dfree, loc, scale = stats.t.fit(rr.values)
                dfree = float(dfree)
                loc = float(loc)
                scale = float(scale)

                # Horizon scaling: mean scales linearly, scale scales with sqrt(days)
                t_q = float(stats.t.ppf(alpha, dfree))  # standard t-quantile
                q_h = (loc * float(days)) + (scale * math.sqrt(float(days)) * t_q)
                var = -q_h

                # ES/CVaR via empirical tail beyond 1-day quantile (robust) then scale
                q1 = float(stats.t.ppf(alpha, dfree, loc=loc, scale=scale))
                tail = rr[rr <= q1]
                if len(tail) >= 10:
                    cvar_1 = -float(tail.mean())
                    cvar = cvar_1 * math.sqrt(float(days))
                else:
                    cvar = float(var)

                if not np.isfinite(var):
                    var = 0.0
                if not np.isfinite(cvar):
                    cvar = 0.0
                return float(max(0.0, var)), float(max(0.0, cvar))
            except Exception:
                pass

        # Normal parametric
        z = float(stats.norm.ppf(alpha))
        q_h = mu_h + z * sigma_h
        var = -(q_h)

        pdf = float(stats.norm.pdf(z))
        cvar = (-mu_h) + sigma_h * (pdf / max(alpha, 1e-12))

        if not np.isfinite(var):
            var = 0.0
        if not np.isfinite(cvar):
            cvar = 0.0
        return float(max(0.0, var)), float(max(0.0, cvar))


    def _capm_beta_alpha(self, r: pd.Series, bench: Optional[pd.Series]) -> Tuple[float, float]:
        if bench is None or bench.dropna().empty:
            return 0.0, 0.0
        aligned = pd.DataFrame({"a": r, "m": bench}).dropna()
        if len(aligned) < 30:
            return 0.0, 0.0
        cov = aligned["a"].cov(aligned["m"])
        var_m = aligned["m"].var()
        beta = cov / var_m if var_m > 1e-12 else 0.0
        ann_a = _annualize_mean(aligned["a"].mean(), self.cfg.annual_trading_days)
        ann_m = _annualize_mean(aligned["m"].mean(), self.cfg.annual_trading_days)
        alpha = (ann_a - self.cfg.risk_free_rate) - beta * (ann_m - self.cfg.risk_free_rate)
        return float(beta), float(alpha)

    def _calculate_treynor_ratio(self, returns: pd.Series, benchmark_returns: Optional[pd.Series] = None) -> float:
        if benchmark_returns is None or benchmark_returns.dropna().empty:
            return 0.0
        aligned = pd.DataFrame({"a": returns, "m": benchmark_returns}).dropna()
        if len(aligned) < 30:
            return 0.0
        cov = aligned["a"].cov(aligned["m"])
        var_m = aligned["m"].var()
        beta = cov / var_m if var_m > 1e-12 else 0.0
        if abs(beta) < 1e-12:
            return 0.0
        ann_ret = _annualize_mean(aligned["a"].mean(), self.cfg.annual_trading_days)
        return float((ann_ret - self.cfg.risk_free_rate) / beta)

    def _calculate_information_ratio(self, returns: pd.Series, benchmark_returns: Optional[pd.Series] = None) -> float:
        if benchmark_returns is None or benchmark_returns.dropna().empty:
            return 0.0
        aligned = pd.DataFrame({"a": returns, "b": benchmark_returns}).dropna()
        if len(aligned) < 30:
            return 0.0
        active = aligned["a"] - aligned["b"]
        te = active.std(ddof=1) * math.sqrt(self.cfg.annual_trading_days)
        if te < 1e-12:
            return 0.0
        ann_active = _annualize_mean(active.mean(), self.cfg.annual_trading_days)
        return float(ann_active / te)

    def _tracking_error(self, returns: pd.Series, benchmark_returns: Optional[pd.Series]) -> float:
        if benchmark_returns is None or benchmark_returns.dropna().empty:
            return 0.0
        aligned = pd.DataFrame({"a": returns, "b": benchmark_returns}).dropna()
        if len(aligned) < 30:
            return 0.0
        active = aligned["a"] - aligned["b"]
        te = active.std(ddof=1) * math.sqrt(self.cfg.annual_trading_days)
        return float(te)

    def rolling_beta(self, returns: pd.Series, benchmark_returns: pd.Series, window: int) -> pd.Series:
        df = pd.DataFrame({"a": returns, "m": benchmark_returns}).dropna()
        if df.shape[0] < window + 5:
            return pd.Series(index=returns.index, dtype=float)
        cov = df["a"].rolling(window).cov(df["m"])
        var = df["m"].rolling(window).var()
        beta = cov / (var + 1e-12)
        return beta.reindex(returns.index)

    def rolling_tracking_error(self, returns: pd.Series, benchmark_returns: pd.Series, window: int) -> pd.Series:
        df = pd.DataFrame({"a": returns, "b": benchmark_returns}).dropna()
        if df.shape[0] < window + 5:
            return pd.Series(index=returns.index, dtype=float)
        active = df["a"] - df["b"]
        te = active.rolling(window).std(ddof=1) * math.sqrt(self.cfg.annual_trading_days)
        return te.reindex(returns.index)



    # =========================================================================
    # FAST / VECTORIZED ROLLING (performance upgrades, Streamlit Cloud-friendly)
    # =========================================================================

    def rolling_beta_many(self, returns_df: pd.DataFrame, benchmark_returns: pd.Series, window: int) -> pd.DataFrame:
        """Compute rolling beta for many assets vs a single benchmark in a single pass.

        This avoids repeated DataFrame alignments inside per-asset loops and is substantially faster
        when the user selects a larger universe.
        """
        if returns_df is None or returns_df.empty or benchmark_returns is None or benchmark_returns.dropna().empty:
            return pd.DataFrame()

        w = int(max(5, window))
        # Listwise alignment for consistent window sample size across assets
        df = pd.concat([returns_df, benchmark_returns.rename("_BENCH_")], axis=1).replace([np.inf, -np.inf], np.nan).dropna(how="any")
        if df.shape[0] < w + 5:
            return pd.DataFrame(index=returns_df.index, columns=returns_df.columns, dtype=float)

        m = df["_BENCH_"].astype(float)
        X = df.drop(columns=["_BENCH_"]).astype(float)

        # Rolling means
        m_mean = m.rolling(w).mean()
        x_mean = X.rolling(w).mean()
        xm_mean = (X.mul(m, axis=0)).rolling(w).mean()

        # Sample covariance from population covariance: cov_s = cov_pop * n/(n-1)
        n = float(w)
        cov_pop = xm_mean - x_mean.mul(m_mean, axis=0)
        cov_s = cov_pop * (n / max(1.0, (n - 1.0)))

        var_m = m.rolling(w).var(ddof=1)
        beta = cov_s.div(var_m.replace(0.0, np.nan), axis=0)

        # Reindex to original calendar
        beta = beta.reindex(returns_df.index)
        return beta

    def rolling_tracking_error_many(self, returns_df: pd.DataFrame, benchmark_returns: pd.Series, window: int) -> pd.DataFrame:
        """Compute rolling tracking error for many assets vs a benchmark in a single pass."""
        if returns_df is None or returns_df.empty or benchmark_returns is None or benchmark_returns.dropna().empty:
            return pd.DataFrame()

        w = int(max(5, window))
        df = pd.concat([returns_df, benchmark_returns.rename("_BENCH_")], axis=1).replace([np.inf, -np.inf], np.nan).dropna(how="any")
        if df.shape[0] < w + 5:
            return pd.DataFrame(index=returns_df.index, columns=returns_df.columns, dtype=float)

        active = df.drop(columns=["_BENCH_"]).sub(df["_BENCH_"], axis=0)
        te = active.rolling(w).std(ddof=1) * math.sqrt(self.cfg.annual_trading_days)
        return te.reindex(returns_df.index)

    def rolling_relative_var_cvar_es(
        self,
        returns: pd.Series,
        benchmark_returns: pd.Series,
        window: int,
        confidence: float,
        horizon: int
    ) -> pd.DataFrame:
        """Vectorized rolling Relative VaR/CVaR/ES for active returns (asset - benchmark).

        Output matches the scale used in relative_var_cvar_es(): annualized % proxy via sqrt(annual_trading_days)*100.
        """
        if returns is None or returns.dropna().empty or benchmark_returns is None or benchmark_returns.dropna().empty:
            return pd.DataFrame()

        w = int(max(30, window))
        h = int(max(1, horizon))
        conf = float(confidence)
        alpha = max(1e-6, 1.0 - conf)

        df = pd.DataFrame({"a": returns, "b": benchmark_returns}).replace([np.inf, -np.inf], np.nan).dropna(how="any")
        if df.shape[0] < w + 10:
            return pd.DataFrame()

        active = (df["a"] - df["b"]).astype(float).values

        try:
            from numpy.lib.stride_tricks import sliding_window_view
            W = sliding_window_view(active, window_shape=w)
        except Exception:
            # Fallback: safe but slower
            W = np.vstack([active[i - w:i] for i in range(w, len(active) + 1)])

        # Historical (distribution-free)
        W_h = W * math.sqrt(h)
        q = np.quantile(W_h, alpha, axis=1)
        var_hist = -q

        # CVaR/ES: mean of tail <= q (vectorized mask)
        mask = W_h <= q[:, None]
        tail_sum = (W_h * mask).sum(axis=1)
        tail_cnt = mask.sum(axis=1)
        tail_mean = np.where(tail_cnt > 0, tail_sum / np.maximum(tail_cnt, 1), q)
        cvar_hist = -tail_mean

        # Parametric (analytic)
        mu = W.mean(axis=1)
        sigma = W.std(axis=1, ddof=1)
        mu_h = mu * h
        sigma_h = sigma * math.sqrt(h)

        if bool(getattr(self.cfg, "use_student_t_parametric", True)):
            dfree = max(5, w - 1)
            q_t = stats.t.ppf(alpha, dfree)
            pdf_t = stats.t.pdf(q_t, dfree)

            var_p = -(mu_h + sigma_h * q_t)

            # ES for left tail of standardized Student-t:
            # E[T | T <= q] = -((df + q^2)/(df-1)) * pdf(q) / alpha   (negative)
            # CVaR(loss) = -E[R | R <= q] = - (mu_h + sigma_h * E[T|...])
            K = ((dfree + q_t ** 2) / max(1.0, (dfree - 1.0))) * (pdf_t / alpha)  # positive
            cvar_p = -mu_h + sigma_h * K
        else:
            z = stats.norm.ppf(alpha)
            pdf = stats.norm.pdf(z)
            var_p = -(mu_h + sigma_h * z)
            cvar_p = -mu_h + sigma_h * (pdf / alpha)

        # Annualize proxy
        scale = math.sqrt(self.cfg.annual_trading_days) * 100.0
        out = pd.DataFrame(
            {
                "Rel_Hist_VaR": var_hist * scale,
                "Rel_Hist_CVaR_ES": cvar_hist * scale,
                "Rel_Param_VaR": var_p * scale,
                "Rel_Param_CVaR": cvar_p * scale,
            },
            index=df.index[w - 1:]
        )
        out.index.name = "Date"
        return out
    def relative_var_cvar_es(self, returns: pd.Series, benchmark_returns: pd.Series, confidence: float, horizon: int) -> Dict[str, float]:
        df = pd.DataFrame({"a": returns, "b": benchmark_returns}).dropna()
        if df.shape[0] < 60:
            return {"Rel_Hist_VaR": 0.0, "Rel_Hist_CVaR_ES": 0.0, "Rel_Param_VaR": 0.0, "Rel_Param_CVaR": 0.0}
        active = df["a"] - df["b"]
        var_h, cvar_h = self._historical_var_cvar(active, confidence, horizon)
        var_p, cvar_p = self._parametric_var_cvar(active, confidence, horizon, use_t=self.cfg.use_student_t_parametric)
        scale = math.sqrt(self.cfg.annual_trading_days)
        return {
            "Rel_Hist_VaR": float(var_h * scale * 100.0),
            "Rel_Hist_CVaR_ES": float(cvar_h * scale * 100.0),
            "Rel_Param_VaR": float(var_p * scale * 100.0),
            "Rel_Param_CVaR": float(cvar_p * scale * 100.0),
        }

# =============================================================================
# VISUALIZATION ENGINE
# =============================================================================
class ScientificVisualizationEngine:
    def __init__(self, cfg: ScientificAnalysisConfiguration):
        self.cfg = cfg

    def _create_empty_plot(self, message: str) -> go.Figure:
        fig = go.Figure()
        fig.add_annotation(text=message, x=0.5, y=0.5, showarrow=False, font=dict(size=16))
        fig.update_layout(height=420, template="plotly_white")
        return fig

    def create_correlation_heatmap(self, corr: pd.DataFrame, title: str) -> go.Figure:
        if corr is None or corr.empty:
            return self._create_empty_plot("No correlation data.")
        fig = px.imshow(
            corr,
            text_auto=False,
            aspect="auto",
            origin="lower",
            color_continuous_scale="RdBu",
            zmin=-1, zmax=1
        )
        fig.update_layout(title=dict(text=title, x=0.5), height=720, template="plotly_white")
        return fig

    def create_volatility_ratio_signal_chart(self, features_df: pd.DataFrame, symbol: str, green_max: float, orange_max: float, title: str) -> go.Figure:
        if features_df is None or features_df.empty:
            return self._create_empty_plot("No features data.")
        if "EWMA_Vol_Ratio_22_over_33_99" not in features_df.columns:
            return self._create_empty_plot("EWMA ratio missing. Run analysis.")
        df = features_df.dropna(subset=["EWMA_Vol_Ratio_22_over_33_99"]).copy()
        if df.empty:
            return self._create_empty_plot("No valid EWMA ratio data.")
        ratio = df["EWMA_Vol_Ratio_22_over_33_99"]
        if "EWMA_Ratio_BB_Upper" not in df.columns or "EWMA_Ratio_BB_Lower" not in df.columns:
            bb_n = 20
            mid = ratio.rolling(bb_n, min_periods=int(bb_n * 0.8)).mean()
            sd = ratio.rolling(bb_n, min_periods=int(bb_n * 0.8)).std()
            df["EWMA_Ratio_BB_Mid"] = mid
            df["EWMA_Ratio_BB_Upper"] = mid + 2.0 * sd
            df["EWMA_Ratio_BB_Lower"] = mid - 2.0 * sd

        fig = go.Figure()
        ymax = max(1.5, float(ratio.max()) * 1.15) if ratio.notna().any() else 1.5

        fig.add_hrect(y0=0.0, y1=green_max, fillcolor="rgba(34,197,94,0.12)", line_width=0,
                      annotation_text="GREEN", annotation_position="top left")
        fig.add_hrect(y0=green_max, y1=orange_max, fillcolor="rgba(245,158,11,0.12)", line_width=0,
                      annotation_text="ORANGE", annotation_position="top left")
        fig.add_hrect(y0=orange_max, y1=ymax, fillcolor="rgba(239,68,68,0.12)", line_width=0,
                      annotation_text="RED", annotation_position="top left")

        fig.add_trace(go.Scatter(x=df.index, y=ratio, name="EWMA Vol Ratio (22 / (33+99))", mode="lines", line=dict(width=2)))
        fig.add_trace(go.Scatter(x=df.index, y=df["EWMA_Ratio_BB_Upper"], name="BB Upper", mode="lines", line=dict(width=1.5, dash="dash")))
        fig.add_trace(go.Scatter(x=df.index, y=df["EWMA_Ratio_BB_Lower"], name="BB Lower", mode="lines", line=dict(width=1.5, dash="dash")))
        fig.add_hline(y=green_max, line_dash="dot", opacity=0.6, annotation_text=f"Green max = {green_max:.2f}")
        fig.add_hline(y=orange_max, line_dash="dot", opacity=0.6, annotation_text=f"Orange max = {orange_max:.2f}")

        last_x = df.index[-1]
        last_y = float(ratio.iloc[-1])
        fig.add_trace(go.Scatter(x=[last_x], y=[last_y], mode="markers", name="Latest", marker=dict(size=10)))

        fig.update_layout(
            title=dict(text=f"{title} — {symbol}", x=0.5),
            template="plotly_white",
            height=650,
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0.0)
        )
        fig.update_yaxes(title_text="Ratio (unitless)", range=[0, ymax])
        fig.update_xaxes(title_text="Date")
        return fig

    def create_tracking_error_chart(self, te_series: pd.Series, symbol: str, green_max: float, orange_max: float, title: str = "Tracking Error (Annualized)") -> go.Figure:
        if te_series is None or te_series.dropna().empty:
            return self._create_empty_plot("No tracking error series available.")
        s = te_series.dropna().astype(float)
        ymax = max(float(s.max()) * 1.25, orange_max * 1.4, 0.10)

        fig = go.Figure()
        fig.add_hrect(y0=0.0, y1=green_max, fillcolor="rgba(34,197,94,0.12)", line_width=0,
                      annotation_text="GREEN", annotation_position="top left")
        fig.add_hrect(y0=green_max, y1=orange_max, fillcolor="rgba(245,158,11,0.12)", line_width=0,
                      annotation_text="ORANGE", annotation_position="top left")
        fig.add_hrect(y0=orange_max, y1=ymax, fillcolor="rgba(239,68,68,0.12)", line_width=0,
                      annotation_text="RED", annotation_position="top left")

        fig.add_trace(go.Scatter(x=s.index, y=s.values, mode="lines", name="Tracking Error", line=dict(width=2)))
        fig.add_hline(y=green_max, line_dash="dot", opacity=0.6, annotation_text=f"Green max = {green_max:.2%}")
        fig.add_hline(y=orange_max, line_dash="dot", opacity=0.6, annotation_text=f"Orange max = {orange_max:.2%}")
        fig.add_trace(go.Scatter(x=[s.index[-1]], y=[float(s.iloc[-1])], mode="markers", name="Latest", marker=dict(size=10)))

        fig.update_layout(
            title=dict(text=f"{title} — {symbol}", x=0.5),
            template="plotly_white",
            height=650,
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0.0)
        )
        fig.update_yaxes(title_text="Tracking Error (decimal)", range=[0, ymax])
        fig.update_xaxes(title_text="Date")
        return fig

    def create_rolling_beta_chart(self, beta: pd.Series, symbol: str, title: str = "Rolling Beta") -> go.Figure:
        if beta is None or beta.dropna().empty:
            return self._create_empty_plot("No rolling beta available.")
        s = beta.dropna().astype(float)
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=s.index, y=s.values, mode="lines", name="Rolling Beta", line=dict(width=2)))
        fig.add_trace(go.Scatter(x=[s.index[-1]], y=[float(s.iloc[-1])], mode="markers", name="Latest", marker=dict(size=10)))
        fig.add_hline(y=1.0, line_dash="dot", opacity=0.6, annotation_text="Beta = 1.0")
        fig.update_layout(
            title=dict(text=f"{title} — {symbol}", x=0.5),
            template="plotly_white",
            height=650,
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0.0)
        )
        fig.update_yaxes(title_text="Beta")
        fig.update_xaxes(title_text="Date")
        return fig

    def create_relative_risk_chart(self, rel_risk_df: pd.DataFrame, symbol: str, green_max: float, orange_max: float, title: str = "Relative Risk vs Benchmark") -> go.Figure:
        if rel_risk_df is None or rel_risk_df.empty:
            return self._create_empty_plot("No relative risk history available.")
        df = rel_risk_df.dropna(how="all").copy()
        if df.empty:
            return self._create_empty_plot("No valid relative risk values.")
        series_candidates = [c for c in ["Rel_Hist_VaR", "Rel_Hist_CVaR_ES", "Rel_Param_VaR", "Rel_Param_CVaR"] if c in df.columns]
        if not series_candidates:
            return self._create_empty_plot("Relative risk columns missing.")
        primary = series_candidates[0]
        s = df[primary].dropna()
        ymax = max(float(s.max()) * 1.25, orange_max * 1.4, 3.0)

        fig = go.Figure()
        fig.add_hrect(y0=0.0, y1=green_max, fillcolor="rgba(34,197,94,0.12)", line_width=0,
                      annotation_text="GREEN", annotation_position="top left")
        fig.add_hrect(y0=green_max, y1=orange_max, fillcolor="rgba(245,158,11,0.12)", line_width=0,
                      annotation_text="ORANGE", annotation_position="top left")
        fig.add_hrect(y0=orange_max, y1=ymax, fillcolor="rgba(239,68,68,0.12)", line_width=0,
                      annotation_text="RED", annotation_position="top left")

        for c in series_candidates:
            fig.add_trace(go.Scatter(
                x=df.index, y=df[c], mode="lines",
                name=c.replace("_", " "),
                line=dict(width=2 if c == primary else 1.5, dash="solid" if c == primary else "dash")
            ))
        fig.add_hline(y=green_max, line_dash="dot", opacity=0.6, annotation_text=f"Green max = {green_max:.2f}%")
        fig.add_hline(y=orange_max, line_dash="dot", opacity=0.6, annotation_text=f"Orange max = {orange_max:.2f}%")
        last = df[primary].dropna()
        if not last.empty:
            fig.add_trace(go.Scatter(x=[last.index[-1]], y=[float(last.iloc[-1])], mode="markers", name="Latest (primary)", marker=dict(size=10)))

        fig.update_layout(
            title=dict(text=f"{title} — {symbol}", x=0.5),
            template="plotly_white",
            height=650,
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0.0)
        )
        fig.update_yaxes(title_text="Relative Risk (annualized %, proxy)", range=[0, ymax])
        fig.update_xaxes(title_text="Date")
        return fig

# =============================================================================
# MAIN PLATFORM
# =============================================================================
class ScientificCommoditiesPlatform:
    def __init__(self):
        self.cfg = ScientificAnalysisConfiguration()
        self.data_manager = ScientificDataManager(self.cfg)
        self.corr_engine = ScientificCorrelationEngine(self.cfg)
        self.analytics = ScientificAnalyticsEngine(self.cfg)
        self.viz = ScientificVisualizationEngine(self.cfg)

        if "selected_assets" not in st.session_state:
            st.session_state.selected_assets = ["GC=F", "SI=F", "CL=F", "HG=F"]
        if "selected_benchmarks" not in st.session_state:
            st.session_state.selected_benchmarks = ["^GSPC"]
        if "sc_results" not in st.session_state:
            st.session_state.sc_results = {}

        if "vol_ratio_thresholds" not in st.session_state:
            st.session_state.vol_ratio_thresholds = {"green_max": self.cfg.vol_ratio_green_max, "orange_max": self.cfg.vol_ratio_orange_max}
        if "te_thresholds" not in st.session_state:
            st.session_state.te_thresholds = {"green_max": self.cfg.te_green_max, "orange_max": self.cfg.te_orange_max}
        if "relrisk_thresholds" not in st.session_state:
            st.session_state.relrisk_thresholds = {"green_max": self.cfg.relvar_green_max, "orange_max": self.cfg.relvar_orange_max}

    def render_sidebar(self):
        st.sidebar.markdown("## ⚙️ Configuration")

        st.sidebar.markdown("### 📌 Asset Universe")
        default_assets = st.session_state.selected_assets
        assets = st.sidebar.multiselect(
            "Assets (tickers)",
            options=[
                "GC=F", "SI=F", "CL=F", "NG=F", "HG=F", "PL=F", "PA=F",
                "ZW=F", "ZC=F", "ZS=F", "KC=F", "CC=F",
                "^BCOM", "DX-Y.NYB"
            ],
            default=default_assets,
            help="Commodities futures (Yahoo tickers). Add/remove as needed.",
            key="assets_multiselect"
        )
        if len(assets) == 0:
            assets = default_assets
        st.session_state.selected_assets = assets

        st.sidebar.markdown("### 🧭 Benchmark")
        bench = st.sidebar.selectbox(
            "Benchmark (market proxy for Beta/Treynor/IR/TE/Relative Risk)",
            options=["^GSPC", "^NDX", "DXY", "XU100.IS", "^BSESN", "^N225"],
            index=0,
            key="benchmark_select"
        )
        st.session_state.selected_benchmarks = [bench]

        st.sidebar.markdown("### 🗓️ Time Range")
        lookback_years = st.sidebar.slider("Lookback Years", 1, 15, int(self.cfg.lookback_years), 1, key="lookback_years")
        self.cfg.lookback_years = lookback_years

        st.sidebar.markdown("### 🔗 Correlation Controls")
        corr_method = st.sidebar.selectbox(
            "Correlation Method",
            options=["pearson", "spearman", "kendall", "ewma", "ledoit_wolf"],
            index=0,
            help="ledoit_wolf requires scikit-learn. ewma uses decay lambda.",
            key="corr_method"
        )
        self.cfg.correlation_method = corr_method
        self.cfg.ensure_psd_corr = st.sidebar.checkbox("Force PSD correlation", value=True, key="psd_corr")
        if corr_method == "ewma":
            self.cfg.ewma_lambda = st.sidebar.slider("EWMA Lambda (decay)", 0.80, 0.99, float(self.cfg.ewma_lambda), 0.01, key="ewma_lambda")

        st.sidebar.markdown("### 📉 VaR / CVaR / ES")
        self.cfg.var_confidence = st.sidebar.slider("Confidence Level", 0.90, 0.99, float(self.cfg.var_confidence), 0.01, key="var_conf")
        self.cfg.var_horizon_days = st.sidebar.slider("Horizon (days)", 1, 20, int(self.cfg.var_horizon_days), 1, key="var_hor")
        self.cfg.use_student_t_parametric = st.sidebar.checkbox("Student-t Parametric VaR", value=True, key="use_t")

        st.sidebar.markdown("### 🧮 Rolling Windows")
        self.cfg.rolling_beta_window = st.sidebar.slider("Rolling Beta Window (days)", 20, 252, int(self.cfg.rolling_beta_window), 1, key="beta_win")
        self.cfg.tracking_error_window = st.sidebar.slider("Tracking Error Window (days)", 20, 252, int(self.cfg.tracking_error_window), 1, key="te_win")

        st.sidebar.markdown("---")
        st.sidebar.markdown("### 🟢🟠🔴 Vol Ratio Risk Bands")
        gmax = st.sidebar.slider("Green max threshold (Ratio)", 0.10, 1.00, float(st.session_state.vol_ratio_thresholds.get("green_max", 0.35)), 0.01, key="vr_green")
        omax = st.sidebar.slider("Orange max threshold (Ratio)", min(1.50, gmax + 0.01), 1.50, max(float(st.session_state.vol_ratio_thresholds.get("orange_max", 0.55)), gmax + 0.01), 0.01, key="vr_orange")
        st.session_state.vol_ratio_thresholds = {"green_max": gmax, "orange_max": omax}

        st.sidebar.markdown("### 🟢🟠🔴 Tracking Error Bands (Annualized)")
        tg = st.sidebar.slider("Green max (TE)", 0.01, 0.20, float(st.session_state.te_thresholds.get("green_max", 0.04)), 0.005, key="te_green")
        to = st.sidebar.slider("Orange max (TE)", min(0.30, tg + 0.005), 0.30, max(float(st.session_state.te_thresholds.get("orange_max", 0.08)), tg + 0.005), 0.005, key="te_orange")
        st.session_state.te_thresholds = {"green_max": tg, "orange_max": to}

        st.sidebar.markdown("### 🟢🟠🔴 Relative Risk Bands (Annualized %)")
        rg = st.sidebar.slider("Green max (Relative risk %)", 0.25, 5.0, float(st.session_state.relrisk_thresholds.get("green_max", 1.0)), 0.05, key="rr_green")
        ro = st.sidebar.slider("Orange max (Relative risk %)", min(10.0, rg + 0.05), 10.0, max(float(st.session_state.relrisk_thresholds.get("orange_max", 2.0)), rg + 0.05), 0.05, key="rr_orange")
        st.session_state.relrisk_thresholds = {"green_max": rg, "orange_max": ro}

        st.sidebar.markdown("---")
        st.sidebar.markdown("### ▶️ Execute")
        run = st.sidebar.button("Run Scientific Analysis", key="run_analysis_btn")
        return run

    def run_scientific_analysis(self):
        assets = st.session_state.selected_assets
        bench = st.session_state.selected_benchmarks[0] if st.session_state.selected_benchmarks else "^GSPC"
        tickers = list(dict.fromkeys(list(assets) + [bench]))

        end = datetime.utcnow().date()
        start = end - timedelta(days=int(self.cfg.lookback_years * 365.25))

        prices = self.data_manager.fetch_prices(tuple(tickers), start=str(start), end=str(end), interval=self.cfg.interval)
        if prices is None or prices.empty:
            st.error("❌ No data downloaded. Please check tickers / internet / Yahoo availability.")
            return

        returns = self.data_manager.compute_returns(prices)

        # Basic return hygiene (no infinities, stable ordering)
        returns = returns.replace([np.inf, -np.inf], np.nan)
        try:
            returns = returns.sort_index()
        except Exception:
            pass

        bench_ret = None
        if bench in returns.columns:
            bench_ret = returns[bench].dropna()

        features: Dict[str, pd.DataFrame] = {}
        metrics: Dict[str, Dict[str, Any]] = {}
        rolling_beta: Dict[str, pd.Series] = {}
        rolling_te: Dict[str, pd.Series] = {}
        relrisk_hist: Dict[str, pd.DataFrame] = {}

        # ------------------------------------------------------------
        # Pre-align returns for stable, high-performance rolling stats
        # ------------------------------------------------------------
        available_assets = [a for a in assets if a in returns.columns]
        betas_df = pd.DataFrame()
        te_df = pd.DataFrame()
        if bench_ret is not None and (not bench_ret.empty) and len(available_assets) > 0:
            try:
                betas_df = self.analytics.rolling_beta_many(returns[available_assets], bench_ret, window=self.cfg.rolling_beta_window)
            except Exception:
                betas_df = pd.DataFrame()
            try:
                te_df = self.analytics.rolling_tracking_error_many(returns[available_assets], bench_ret, window=self.cfg.tracking_error_window)
            except Exception:
                te_df = pd.DataFrame()

        for a in assets:
            if a not in prices.columns:
                continue
            ser = prices[a].dropna()
            feat = self.data_manager.calculate_scientific_features(ser)
            features[a] = feat

            r = returns[a].dropna() if a in returns.columns else pd.Series(dtype=float)
            metrics[a] = self.analytics.calculate_scientific_risk_metrics(r, benchmark_returns=bench_ret)

            if bench_ret is not None and not bench_ret.empty and not r.empty:
                # Prefer vectorized precomputed rolling stats (fast path)
                try:
                    if isinstance(betas_df, pd.DataFrame) and (not betas_df.empty) and (a in betas_df.columns):
                        rolling_beta[a] = betas_df[a]
                    else:
                        rolling_beta[a] = self.analytics.rolling_beta(r, bench_ret, window=self.cfg.rolling_beta_window)
                except Exception:
                    rolling_beta[a] = self.analytics.rolling_beta(r, bench_ret, window=self.cfg.rolling_beta_window)

                try:
                    if isinstance(te_df, pd.DataFrame) and (not te_df.empty) and (a in te_df.columns):
                        rolling_te[a] = te_df[a]
                    else:
                        rolling_te[a] = self.analytics.rolling_tracking_error(r, bench_ret, window=self.cfg.tracking_error_window)
                except Exception:
                    rolling_te[a] = self.analytics.rolling_tracking_error(r, bench_ret, window=self.cfg.tracking_error_window)

                # Rolling relative risk (active returns) — vectorized default + legacy fallback
                win = max(120, int(self.cfg.tracking_error_window))
                try:
                    relrisk_hist[a] = self.analytics.rolling_relative_var_cvar_es(
                        r,
                        bench_ret,
                        window=win,
                        confidence=self.cfg.var_confidence,
                        horizon=self.cfg.var_horizon_days
                    )
                except Exception:
                    # Legacy fallback (kept for robustness; slower)
                    df_ab = pd.DataFrame({"a": r, "b": bench_ret}).dropna()
                    if df_ab.shape[0] >= win + 10:
                        rr_rows, idx = [], []
                        for i in range(win, df_ab.shape[0]):
                            sub = df_ab.iloc[i - win:i]
                            rr_rows.append(self.analytics.relative_var_cvar_es(sub["a"], sub["b"], self.cfg.var_confidence, self.cfg.var_horizon_days))
                            idx.append(sub.index[-1])
                        relrisk_hist[a] = pd.DataFrame(rr_rows, index=pd.Index(idx, name="Date"))
                    else:
                        relrisk_hist[a] = pd.DataFrame()

        corr_in = returns[available_assets].dropna(how="all")
        corr = self.corr_engine.compute_correlation(corr_in, method=self.cfg.correlation_method)

        st.session_state.sc_results = {
            "prices": prices,
            "returns": returns,
            "benchmark": bench,
            "benchmark_returns": bench_ret,
            "features": features,
            "metrics": metrics,
            "corr": corr,
            "rolling_beta": rolling_beta,
            "rolling_te": rolling_te,
            "relrisk_hist": relrisk_hist,
            "config_snapshot": dict(self.cfg.__dict__),
            "timestamp": datetime.utcnow().isoformat()
        }

    def render(self):
        st.markdown(
            """
            <div class="institutional-hero">
              <h1>🏛️ Institutional Commodities Analytics Platform <span class="subtle">v7.2</span></h1>
              <p>Robust correlations • Institutional risk metrics • EWMA volatility risk signal • Tracking Error • Rolling Beta • Relative VaR/CVaR/ES</p>
            </div>
            """,
            unsafe_allow_html=True
        )

        run_clicked = self.render_sidebar()
        if run_clicked:
            with st.spinner("Running scientific analysis..."):
                try:
                    self.run_scientific_analysis()
                    st.success("✅ Analysis complete.")
                except Exception as e:
                    st.error(f"Analysis failed: {e}")
                    st.code(traceback.format_exc())

        tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
            "📊 Overview",
            "📈 Risk Analytics",
            "🧭 EWMA Vol Ratio Signal",
            "🔗 Correlation Analysis",
            "🎯 Tracking Error",
            "🧷 Rolling Beta",
            "⚖️ Relative VaR/CVaR/ES"
        ])

        with tab1:
            self.render_overview()
        with tab2:
            self.render_risk_analytics()
        with tab3:
            self.render_vol_ratio_signal()
        with tab4:
            self.render_correlation_analysis()
        with tab5:
            self.render_tracking_error()
        with tab6:
            self.render_rolling_beta()
        with tab7:
            self.render_relative_risk()

        st.markdown("---")
        self.render_data_validation()

    def render_overview(self):
        st.markdown(
            """
            <div class="section-header">
                <h2>📊 Overview</h2>
                <div class="section-actions">
                    <span class="scientific-badge info">v7.2 Ultra</span>
                    <span class="scientific-badge">SciPy: {}</span>
                    <span class="scientific-badge">Plotly</span>
                </div>
            </div>
            """.format(scipy.__version__),
            unsafe_allow_html=True
        )

        res = st.session_state.get("sc_results", {})
        if not res:
            st.info("Run analysis from the sidebar to populate metrics, correlation, and signal tabs.")
            return

        prices: pd.DataFrame = res.get("prices", pd.DataFrame())
        returns: pd.DataFrame = res.get("returns", pd.DataFrame())
        bench = res.get("benchmark", "")

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.markdown(f"""
            <div class="institutional-card">
                <div class="metric-title">Assets Selected</div>
                <div class="metric-value">{len(st.session_state.selected_assets)}</div>
                <div class="subtle">{", ".join(st.session_state.selected_assets[:4])}{("..." if len(st.session_state.selected_assets)>4 else "")}</div>
            </div>
            """, unsafe_allow_html=True)

        with c2:
            st.markdown(f"""
            <div class="institutional-card">
                <div class="metric-title">Benchmark</div>
                <div class="metric-value">{bench}</div>
                <div class="subtle">Used for Beta/Treynor/IR/TE/Relative Risk</div>
            </div>
            """, unsafe_allow_html=True)

        with c3:
            nobs = int(prices.shape[0]) if prices is not None else 0
            st.markdown(f"""
            <div class="institutional-card">
                <div class="metric-title">Data Points</div>
                <div class="metric-value">{nobs}</div>
                <div class="subtle">{res.get("timestamp","")[:19]} UTC</div>
            </div>
            """, unsafe_allow_html=True)

        with c4:
            st.markdown(f"""
            <div class="institutional-card">
                <div class="metric-title">Correlation Method</div>
                <div class="metric-value">{self.cfg.correlation_method}</div>
                <div class="subtle">PSD enforced: {self.cfg.ensure_psd_corr}</div>
            </div>
            """, unsafe_allow_html=True)

        st.markdown("### Price Snapshot")
        if prices is not None and not prices.empty:
            st.line_chart(prices[st.session_state.selected_assets].dropna(how="all"))
        else:
            st.warning("No prices to display.")

        st.markdown("### Returns Snapshot (last 250)")
        if returns is not None and not returns.empty:
            st.dataframe(returns[st.session_state.selected_assets].tail(250), use_container_width=True)
        else:
            st.warning("No returns to display.")

    def render_risk_analytics(self):
        st.markdown(
            """
            <div class="section-header">
                <h2>📈 Risk Analytics</h2>
                <div class="section-actions">
                    <span class="scientific-badge info">Sharpe • Sortino</span>
                    <span class="scientific-badge medium-risk">VaR/CVaR/ES</span>
                    <span class="scientific-badge">Treynor • IR (real benchmark)</span>
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )

        res = st.session_state.get("sc_results", {})
        if not res:
            st.info("Run analysis first.")
            return

        metrics: Dict[str, Dict[str, Any]] = res.get("metrics", {})
        if not metrics:
            st.warning("No metrics computed.")
            return

        df = pd.DataFrame(metrics).T
        for c in ["Ann_Return", "Ann_Vol", "Hist_VaR", "Hist_CVaR_ES", "Param_VaR", "Param_CVaR", "Tracking_Error"]:
            if c in df.columns:
                df[c] = df[c] * 100.0 if c in ["Ann_Return", "Ann_Vol"] else df[c] * 100.0

        st.dataframe(df.sort_index(), use_container_width=True)

        if "Sharpe" in df.columns:
            st.markdown("### Sharpe Comparison")
            fig = px.bar(df.reset_index().rename(columns={"index": "Asset"}), x="Asset", y="Sharpe")
            fig.update_layout(height=420, template="plotly_white")
            st.plotly_chart(fig, use_container_width=True)

    def render_vol_ratio_signal(self):
        res = st.session_state.get("sc_results", {})
        if not res:
            st.info("Run analysis first.")
            return

        st.markdown(
            """
            <div class="section-header">
                <h2>🧭 EWMA Volatility Ratio Signal</h2>
                <div class="section-actions">
                    <span class="scientific-badge info">(EWMA22)/(EWMA33+EWMA99)</span>
                    <span class="scientific-badge medium-risk">Bollinger Bands</span>
                    <span class="scientific-badge high-risk">Risk Zones</span>
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )

        features: Dict[str, pd.DataFrame] = res.get("features", {})
        symbols = list(features.keys())
        if not symbols:
            st.warning("No features available.")
            return

        symbol = st.selectbox("Select Asset for Signal", options=symbols, index=0, key="vol_ratio_symbol")
        df = features.get(symbol, pd.DataFrame())
        if df is None or df.empty:
            st.warning("No data for selected asset.")
            return

        thr = st.session_state.get("vol_ratio_thresholds", {"green_max": 0.35, "orange_max": 0.55})
        green_max = float(thr.get("green_max", 0.35))
        orange_max = float(thr.get("orange_max", 0.55))

        if "EWMA_Vol_Ratio_22_over_33_99" in df.columns and df["EWMA_Vol_Ratio_22_over_33_99"].dropna().shape[0] > 0:
            last_val = float(df["EWMA_Vol_Ratio_22_over_33_99"].dropna().iloc[-1])
            if last_val <= green_max:
                zone, badge = "GREEN", "low-risk"
            elif last_val <= orange_max:
                zone, badge = "ORANGE", "medium-risk"
            else:
                zone, badge = "RED", "high-risk"

            st.markdown(f"""
            <div class="institutional-card">
                <div class="metric-title">Latest Risk Signal</div>
                <div style="display:flex; justify-content:space-between; align-items:center; gap:1rem;">
                    <div>
                        <div class="metric-value">{last_val:.3f}</div>
                        <div class="subtle">EWMA Vol Ratio (unitless)</div>
                    </div>
                    <div><span class="scientific-badge {badge}">{zone} ZONE</span></div>
                </div>
            </div>
            """, unsafe_allow_html=True)

        fig = self.viz.create_volatility_ratio_signal_chart(df, symbol, green_max, orange_max, "Institutional Risk Signal: EWMA Vol Ratio")
        st.plotly_chart(fig, use_container_width=True)

        with st.expander("📋 Signal Data (Last 80 rows)", expanded=False):
            cols = [c for c in ["EWMA_Vol_22", "EWMA_Vol_33", "EWMA_Vol_99",
                                "EWMA_Vol_Ratio_22_over_33_99",
                                "EWMA_Ratio_BB_Mid", "EWMA_Ratio_BB_Upper", "EWMA_Ratio_BB_Lower"] if c in df.columns]
            st.dataframe(df[cols].tail(80), use_container_width=True)

    def render_correlation_analysis(self):
        res = st.session_state.get("sc_results", {})
        if not res:
            st.info("Run analysis first.")
            return
        st.markdown(
            """
            <div class="section-header">
                <h2>🔗 Correlation Analysis</h2>
                <div class="section-actions">
                    <span class="scientific-badge info">Correct alignment</span>
                    <span class="scientific-badge">PSD safe</span>
                    <span class="scientific-badge">Ledoit-Wolf optional</span>
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )
        corr = res.get("corr", pd.DataFrame())
        if corr is None or corr.empty:
            st.warning("No correlation matrix computed.")
            return
        fig = self.viz.create_correlation_heatmap(corr, f"Asset Correlations — method: {self.cfg.correlation_method}")
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("📋 Correlation Table", expanded=False):
            st.dataframe(corr.round(4), use_container_width=True)

    def render_tracking_error(self):
        res = st.session_state.get("sc_results", {})
        if not res:
            st.info("Run analysis first.")
            return
        st.markdown(
            """
            <div class="section-header">
                <h2>🎯 Tracking Error</h2>
                <div class="section-actions">
                    <span class="scientific-badge info">Active risk vs benchmark</span>
                    <span class="scientific-badge medium-risk">Green/Orange/Red Zones</span>
                    <span class="scientific-badge">Rolling window</span>
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )
        te_map: Dict[str, pd.Series] = res.get("rolling_te", {})
        if not te_map:
            st.warning("Tracking error series not available (need benchmark + enough data).")
            return
        symbols = list(te_map.keys())
        symbol = st.selectbox("Select Asset", options=symbols, index=0, key="te_symbol")
        te_series = te_map.get(symbol, pd.Series(dtype=float))
        thr = st.session_state.get("te_thresholds", {"green_max": 0.04, "orange_max": 0.08})
        green_max, orange_max = float(thr.get("green_max", 0.04)), float(thr.get("orange_max", 0.08))

        if te_series is not None and te_series.dropna().shape[0] > 0:
            last = float(te_series.dropna().iloc[-1])
            if last <= green_max:
                zone, badge = "GREEN", "low-risk"
            elif last <= orange_max:
                zone, badge = "ORANGE", "medium-risk"
            else:
                zone, badge = "RED", "high-risk"
            st.markdown(f"""
            <div class="institutional-card">
                <div class="metric-title">Latest Tracking Error (annualized)</div>
                <div style="display:flex; justify-content:space-between; align-items:center; gap:1rem;">
                    <div>
                        <div class="metric-value">{last:.2%}</div>
                        <div class="subtle">Rolling window: {self.cfg.tracking_error_window} days</div>
                    </div>
                    <div><span class="scientific-badge {badge}">{zone} ZONE</span></div>
                </div>
            </div>
            """, unsafe_allow_html=True)

        fig = self.viz.create_tracking_error_chart(te_series, symbol, green_max, orange_max, f"Tracking Error (Annualized) — Window {self.cfg.tracking_error_window}D")
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("📋 Tracking Error Data (Last 120 rows)", expanded=False):
            st.dataframe(te_series.dropna().to_frame("Tracking_Error").tail(120), use_container_width=True)

    def render_rolling_beta(self):
        res = st.session_state.get("sc_results", {})
        if not res:
            st.info("Run analysis first.")
            return
        st.markdown(
            """
            <div class="section-header">
                <h2>🧷 Rolling Beta</h2>
                <div class="section-actions">
                    <span class="scientific-badge info">Rolling CAPM beta</span>
                    <span class="scientific-badge">Benchmark-linked</span>
                    <span class="scientific-badge">Window configurable</span>
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )
        beta_map: Dict[str, pd.Series] = res.get("rolling_beta", {})
        if not beta_map:
            st.warning("Rolling beta not available (need benchmark + enough data).")
            return
        symbols = list(beta_map.keys())
        symbol = st.selectbox("Select Asset", options=symbols, index=0, key="beta_symbol")
        beta = beta_map.get(symbol, pd.Series(dtype=float))
        if beta is None or beta.dropna().empty:
            st.warning("No rolling beta data for this asset.")
            return
        last = float(beta.dropna().iloc[-1])
        st.markdown(f"""
        <div class="institutional-card">
            <div class="metric-title">Latest Rolling Beta</div>
            <div style="display:flex; justify-content:space-between; align-items:center; gap:1rem;">
                <div>
                    <div class="metric-value">{last:.3f}</div>
                    <div class="subtle">Window: {self.cfg.rolling_beta_window} days</div>
                </div>
                <div><span class="scientific-badge info">Beta vs {res.get("benchmark","")}</span></div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        fig = self.viz.create_rolling_beta_chart(beta, symbol, f"Rolling Beta — Window {self.cfg.rolling_beta_window}D")
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("📋 Beta Data (Last 120 rows)", expanded=False):
            st.dataframe(beta.dropna().to_frame("Rolling_Beta").tail(120), use_container_width=True)

    def render_relative_risk(self):
        res = st.session_state.get("sc_results", {})
        if not res:
            st.info("Run analysis first.")
            return
        st.markdown(
            """
            <div class="section-header">
                <h2>⚖️ Relative VaR / CVaR / ES vs Benchmark</h2>
                <div class="section-actions">
                    <span class="scientific-badge info">Active returns risk</span>
                    <span class="scientific-badge medium-risk">Band zones</span>
                    <span class="scientific-badge">Rolling history</span>
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )
        rel_map: Dict[str, pd.DataFrame] = res.get("relrisk_hist", {})
        if not rel_map:
            st.warning("Relative risk history not available.")
            return
        symbols = [s for s, df in rel_map.items() if df is not None]
        if not symbols:
            st.warning("No relative risk frames computed.")
            return
        symbol = st.selectbox("Select Asset", options=symbols, index=0, key="rel_symbol")
        df = rel_map.get(symbol, pd.DataFrame())
        if df is None or df.empty:
            st.warning("Not enough data for rolling relative risk. Increase lookback or reduce window.")
            return
        thr = st.session_state.get("relrisk_thresholds", {"green_max": 1.0, "orange_max": 2.0})
        green_max, orange_max = float(thr.get("green_max", 1.0)), float(thr.get("orange_max", 2.0))

        primary = "Rel_Hist_VaR" if "Rel_Hist_VaR" in df.columns else df.columns[0]
        last = df[primary].dropna()
        if not last.empty:
            last_val = float(last.iloc[-1])
            if last_val <= green_max:
                zone, badge = "GREEN", "low-risk"
            elif last_val <= orange_max:
                zone, badge = "ORANGE", "medium-risk"
            else:
                zone, badge = "RED", "high-risk"
            st.markdown(f"""
            <div class="institutional-card">
                <div class="metric-title">Latest Relative Risk (primary: {primary})</div>
                <div style="display:flex; justify-content:space-between; align-items:center; gap:1rem;">
                    <div>
                        <div class="metric-value">{last_val:.2f}%</div>
                        <div class="subtle">Annualized risk proxy from active returns</div>
                    </div>
                    <div><span class="scientific-badge {badge}">{zone} ZONE</span></div>
                </div>
            </div>
            """, unsafe_allow_html=True)

        fig = self.viz.create_relative_risk_chart(df, symbol, green_max, orange_max, f"Relative VaR/CVaR/ES vs {res.get('benchmark','')}")
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("📋 Relative Risk Data (Last 120 rows)", expanded=False):
            st.dataframe(df.tail(120), use_container_width=True)

    def render_data_validation(self):
        res = st.session_state.get("sc_results", {})
        st.markdown(
            """
            <div class="section-header">
                <h2>📋 Data & Validation</h2>
                <div class="section-actions">
                    <span class="scientific-badge">Quality checks</span>
                    <span class="scientific-badge">Overlap / NA</span>
                    <span class="scientific-badge">Diagnostics</span>
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )
        if not res:
            st.info("No results yet.")
            return

        prices: pd.DataFrame = res.get("prices", pd.DataFrame())
        returns: pd.DataFrame = res.get("returns", pd.DataFrame())
        assets = st.session_state.selected_assets
        bench = res.get("benchmark", "")

        c1, c2 = st.columns(2)
        with c1:
            st.markdown("#### Missingness (Prices)")
            if prices is not None and not prices.empty:
                miss = (prices[assets + [bench]].isna().mean() * 100.0).sort_values(ascending=False)
                st.dataframe(miss.to_frame("Missing %").round(2), use_container_width=True)
            else:
                st.warning("No price data.")
        with c2:
            st.markdown("#### Missingness (Returns)")
            if returns is not None and not returns.empty:
                miss = (returns[assets + [bench]].isna().mean() * 100.0).sort_values(ascending=False)
                st.dataframe(miss.to_frame("Missing %").round(2), use_container_width=True)
            else:
                st.warning("No return data.")

        st.markdown("#### Notes")
        st.write(
            "- Correlations computed after aligning returns and dropping all-NA columns.\n"
            "- PSD enforcement ensures correlation matrix is numerically valid for risk engines.\n"
            "- Treynor / Information Ratio / Tracking Error / Rolling Beta are computed vs selected benchmark.\n"
            "- Relative VaR/CVaR/ES uses active returns (asset - benchmark) and is shown as an annualized % proxy."
        )

# =============================================================================
# MAIN
# =============================================================================
def main():
    try:
        app = ScientificCommoditiesPlatform()
        app.render()
    except Exception as e:
        st.error(f"Fatal error: {e}")
        st.code(traceback.format_exc())

# NOTE: Streamlit router below controls which app runs (merged build).


# =============================================================================
# 🔥 ULTRA MERGE PATCHES (v7.3) — Fill missing InstitutionalCommoditiesDashboard tabs
# - Robust, correct correlation matrix (aligned, PSD enforced)
# - Optional Ledoit–Wolf shrinkage correlation (scikit-learn; auto-fallback)
# - EWMA Vol Ratio signal tab (22 / (33+99)) with Bollinger + green/orange/red zones
# - Rolling Beta tab (vs benchmark)
# - Risk/Performance tabs (Treynor, Information Ratio, VaR/CVaR/ES) using REAL benchmark
# - Stress testing, Reporting, Settings, Portfolio Lab (PyPortfolioOpt optional)
# =============================================================================

def _icd__safe_df(x):
    import pandas as pd
    if x is None:
        return pd.DataFrame()
    if isinstance(x, pd.DataFrame):
        return x.copy()
    if isinstance(x, dict):
        return pd.DataFrame(x).copy()
    try:
        return pd.DataFrame(x).copy()
    except Exception:
        return pd.DataFrame()

def _icd__get_returns_df(self):
    import pandas as pd
    import streamlit as st
    to_df = getattr(self, "_to_returns_df", None)
    if callable(to_df):
        df = to_df(st.session_state.get("returns_data", None))
    else:
        df = _icd__safe_df(st.session_state.get("returns_data", None))
    if not isinstance(df, pd.DataFrame):
        df = pd.DataFrame()
    df = df.sort_index()
    # Drop all-NA columns; keep numeric only
    df = df.apply(pd.to_numeric, errors="coerce")
    df = df.dropna(axis=1, how="all")
    return df

def _icd__get_benchmark_df(self):
    import pandas as pd
    import streamlit as st
    to_df = getattr(self, "_to_returns_df", None)
    if callable(to_df):
        bdf = to_df(st.session_state.get("benchmark_returns_data", None))
    else:
        bdf = _icd__safe_df(st.session_state.get("benchmark_returns_data", None))
    if not isinstance(bdf, pd.DataFrame):
        bdf = pd.DataFrame()
    bdf = bdf.sort_index()
    bdf = bdf.apply(pd.to_numeric, errors="coerce")
    bdf = bdf.dropna(axis=1, how="all")
    return bdf

def _icd__pick_benchmark_series(bdf, preferred_col=None):
    import pandas as pd
    if bdf is None or not isinstance(bdf, pd.DataFrame) or bdf.empty:
        return pd.Series(dtype=float), None
    cols = list(bdf.columns)
    if preferred_col and preferred_col in cols:
        c = preferred_col
    else:
        c = cols[0]
    s = pd.to_numeric(bdf[c], errors="coerce").dropna()
    return s, c

def _icd__equal_weight_portfolio(returns_df, asset_cols, min_assets_frac: float = 0.60):
    """Equal-weight portfolio returns with robust missing-data handling.

    Why this matters:
    - Different commodity futures have different holiday calendars / missing bars.
    - If you compute an EW portfolio with a naive dot-product, any NaN in a row can turn the whole day into NaN.
    - If you compute a simple mean with skipna=True, you can end up averaging only 1 asset on many dates.
      That can make VaR / correlations look wrong or unstable.

    This implementation:
    - Computes an equal-weight mean with skipna=True
    - Enforces a minimum number of assets present per day:
        min_assets = ceil(min_assets_frac * n_assets)
      so portfolio returns are based on a meaningful basket.
    """
    import pandas as pd
    import numpy as np
    import math

    if returns_df is None or getattr(returns_df, "empty", False):
        return pd.Series(dtype=float)

    cols = [c for c in (asset_cols or []) if c in returns_df.columns]
    if not cols:
        return pd.Series(dtype=float)

    sub = returns_df[cols].copy()
    # Ensure numeric and clean
    try:
        for c in cols:
            sub[c] = pd.to_numeric(sub[c], errors="coerce")
    except Exception:
        sub = sub.apply(lambda s: pd.to_numeric(s, errors="coerce"))

    sub = sub.replace([np.inf, -np.inf], np.nan)

    # Require a minimum number of available asset returns per day
    n_assets = int(len(cols))
    min_assets = int(max(1, math.ceil(float(min_assets_frac) * float(n_assets))))
    available = sub.notna().sum(axis=1)

    port = sub.mean(axis=1, skipna=True)
    port = port.where(available >= min_assets)

    port = pd.to_numeric(port, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    return port

def _icd__compute_correlation_matrix(self, returns_df, method="sample_pairwise", ensure_psd=True):
    """
    Robust correlation matrix for *reporting* and *visualization*.

    Why correlations sometimes show as ~1 (or exactly 1) across many pairs:
    - If you compute correlation on a *tiny overlap* (e.g., 2-5 points), the sample correlation can be ±1
      (two points always lie perfectly on a line).
    - If you do strict complete-case alignment across many assets, the intersection of dates can collapse.
    - If a series is nearly constant (zero variance) after cleaning, corr can be unstable / NaN.

    This implementation:
    - Computes pairwise correlations with a **minimum overlap** (min_corr_obs; default 60).
    - Falls back gracefully when alignment is too strict.
    - Optionally enforces PSD (Higham) *after* sanitizing NaNs/Infs, to prevent optimization crashes.

    Parameters
    ----------
    method:
      - "sample_pairwise" (default): pairwise deletion + min overlap (recommended for reporting)
      - "sample_aligned": complete-case across all assets, but only if enough rows; else falls back to pairwise
      - "ledoit_wolf": Ledoit–Wolf covariance -> corr on complete-case data (requires sklearn); needs enough rows
    """
    import numpy as np
    import pandas as pd

    # --- data hygiene ---
    if returns_df is None:
        return pd.DataFrame()

    df = returns_df.copy()
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.apply(pd.to_numeric, errors="coerce").dropna(axis=1, how="all")

    if df.shape[1] < 2:
        return pd.DataFrame()

    # Minimum overlap gate per pair (prevents tiny overlaps -> corr = ±1)
    # Prefer cfg if present; else use safe default.
    min_obs = int(getattr(getattr(self, "cfg", object()), "min_corr_obs", 60))
    min_obs = max(10, min_obs)

    # Pairwise overlap counts (useful for diagnosing issues)
    try:
        mask = df.notna().astype(np.int8)
        n_obs = (mask.T @ mask).astype(int)
    except Exception:
        n_obs = None

    # --- correlation estimation ---
    corr_df = None

    # Ledoit–Wolf (optional)
    if method == "ledoit_wolf":
        try:
            from sklearn.covariance import LedoitWolf
            df_cc = df.dropna(how="any")
            if df_cc.shape[0] < min_obs:
                raise ValueError("Insufficient aligned observations for Ledoit-Wolf.")
            X = df_cc.values
            lw = LedoitWolf().fit(X)
            cov = np.asarray(lw.covariance_, dtype=float)
            d = np.sqrt(np.clip(np.diag(cov), 1e-18, None))
            denom = np.outer(d, d)
            corr = np.divide(cov, denom, out=np.full_like(cov, np.nan), where=denom > 0)
            corr_df = pd.DataFrame(corr, index=list(df_cc.columns), columns=list(df_cc.columns))
        except Exception:
            # fallback to pairwise
            method = "sample_pairwise"
            corr_df = None

    # Aligned sample correlation (only if enough rows)
    if corr_df is None and method == "sample_aligned":
        df_cc = df.dropna(how="any")
        if df_cc.shape[0] >= min_obs:
            # On aligned data, classic sample correlation is fine
            corr_df = df_cc.corr()
        else:
            # Fall back to pairwise with min overlap (more realistic on multi-market datasets)
            method = "sample_pairwise"

    # Pairwise correlation with minimum overlap
    if corr_df is None:
        # pandas supports min_periods in corr() (modern versions). Keep a manual fallback.
        try:
            corr_df = df.corr(method="pearson", min_periods=min_obs)
        except TypeError:
            # Manual fallback for very old pandas
            cols = list(df.columns)
            corr_mat = np.full((len(cols), len(cols)), np.nan, dtype=float)
            for i, ci in enumerate(cols):
                xi = df[ci]
                for j, cj in enumerate(cols):
                    if j < i:
                        continue
                    xj = df[cj]
                    both = xi.notna() & xj.notna()
                    nn = int(both.sum())
                    if i == j:
                        corr_mat[i, j] = 1.0
                    elif nn >= min_obs:
                        a = xi[both].astype(float).values
                        b = xj[both].astype(float).values
                        sa = np.std(a, ddof=1)
                        sb = np.std(b, ddof=1)
                        if sa > 1e-12 and sb > 1e-12:
                            corr_mat[i, j] = float(np.corrcoef(a, b)[0, 1])
                        else:
                            corr_mat[i, j] = np.nan
                    else:
                        corr_mat[i, j] = np.nan
                    corr_mat[j, i] = corr_mat[i, j]
            corr_df = pd.DataFrame(corr_mat, index=cols, columns=cols)

    # --- sanitize ---
    corr_df = corr_df.copy()
    corr_df = corr_df.replace([np.inf, -np.inf], np.nan)
    # Keep NaNs for reporting (insufficient overlap), but ensure diag = 1
    np.fill_diagonal(corr_df.values, 1.0)
    corr_df = corr_df.clip(-1.0, 1.0)

    # --- Optional PSD enforcement (for risk engines / optimization) ---
    # For visualization, PSD is not strictly necessary; but if requested, sanitize NaNs to 0 for Higham.
    if ensure_psd:
        try:
            corr_work = corr_df.copy()
            corr_work = corr_work.fillna(0.0)
            corr = corr_work.values.astype(float)
            corr = 0.5 * (corr + corr.T)
            np.fill_diagonal(corr, 1.0)

            if hasattr(self, "analytics") and hasattr(self.analytics, "_higham_nearest_correlation"):
                corr = self.analytics._higham_nearest_correlation(corr, max_iter=100, tol=1e-7, epsilon=1e-12)
            else:
                # eigen-clip fallback
                vals, vecs = np.linalg.eigh(corr)
                vals = np.clip(vals, 1e-10, None)
                corr = (vecs @ np.diag(vals) @ vecs.T)
                corr = 0.5 * (corr + corr.T)
                np.fill_diagonal(corr, 1.0)

            corr = np.clip(corr, -1.0, 1.0)
            corr_df = pd.DataFrame(corr, index=list(corr_df.index), columns=list(corr_df.columns))
        except Exception:
            # If PSD enforcement fails, return the non-PSD but realistic pairwise corr_df
            pass

    # Attach overlap counts for debugging if caller wants it
    # (We do not return it to preserve API, but store in self for optional display.)
    try:
        if n_obs is not None:
            self._last_corr_overlap = n_obs
            self._last_corr_min_obs = min_obs
            self._last_corr_method = method
    except Exception:
        pass

    return corr_df
def _icd_display_advanced_analytics_fallback(self, cfg):
    import numpy as np
    import pandas as pd
    import streamlit as st
    import plotly.graph_objects as go

    st.markdown("### 🧠 Advanced Analytics (Institutional)")

    returns_df = _icd__get_returns_df(self)
    bench_df = _icd__get_benchmark_df(self)

    if returns_df.empty:
        st.info("Load data from the sidebar to begin.")
        return

    bench_series, bench_col = _icd__pick_benchmark_series(bench_df)

    # Asset selector (or portfolio)
    scope = st.radio(
        "Scope",
        options=["Equal-Weight Portfolio", "Single Asset"],
        index=0,
        horizontal=True,
        key="adv_scope"
    )

    if scope.startswith("Equal"):
        assets = list(returns_df.columns)
        default_assets = assets[: min(6, len(assets))]
        sel = st.multiselect("Assets (equal weight)", assets, default=default_assets, key="adv_assets")
        series = _icd__equal_weight_portfolio(returns_df, sel)
        label = "EW Portfolio"
    else:
        sym = st.selectbox("Select Asset", options=list(returns_df.columns), index=0, key="adv_asset")
        series = pd.to_numeric(returns_df[sym], errors="coerce").dropna()
        label = sym

    if series.empty or len(series) < 60:
        st.warning("Insufficient data for analytics (need ~60+ observations).")
        return

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Observations", f"{len(series):,}")
    with c2:
        st.metric("Mean (daily)", f"{series.mean():.4%}")
    with c3:
        st.metric("Vol (daily)", f"{series.std():.4%}")
    with c4:
        st.metric("Skew / Kurt", f"{series.skew():.2f} / {series.kurtosis():.2f}")

    # Performance metrics (incl. Treynor/IR if benchmark available)
    bm = None
    if bench_series is not None and not bench_series.empty:
        idx = series.index.intersection(bench_series.index)
        if len(idx) >= 60:
            bm = bench_series.loc[idx]
            s_aligned = series.loc[idx]
        else:
            bm = None
            s_aligned = series
    else:
        s_aligned = series

    try:
        self.analytics.risk_free_rate = float(getattr(cfg, "risk_free_rate", 0.02))
        perf = self.analytics.calculate_performance_metrics(s_aligned, benchmark_returns=bm)
    except Exception:
        perf = {}

    if perf:
        perf_tbl = pd.DataFrame([perf]).T
        perf_tbl.columns = [label]
        st.dataframe(perf_tbl, use_container_width=True)
    else:
        st.info("Performance metrics unavailable (data too short or invalid).")

    # GARCH (optional)
    st.markdown("#### 📉 Volatility Modeling (GARCH)")
    garch_on = st.checkbox("Run GARCH(1,1) volatility estimate", value=False, key="adv_garch_on")
    if garch_on:
        try:
            out = self.analytics.garch_analysis(s_aligned, p=1, q=1)
            if out and out.get("success", False):
                vol = out.get("conditional_volatility")
                if isinstance(vol, pd.Series) and not vol.empty:
                    fig = self.visualizer.create_garch_volatility(s_aligned, conditional_vol=vol.values, title=f"GARCH Volatility — {label}")
                    st.plotly_chart(fig, use_container_width=True)
                st.json({k: v for k, v in out.items() if k not in ("conditional_volatility",)}, expanded=False)
            else:
                st.warning(out.get("message", "GARCH analysis returned no result."))
        except Exception as e:
            st.warning(f"GARCH failed: {e}")

    # Regime detection (optional)
    st.markdown("#### 🧩 Regime Detection (HMM optional)")
    reg_on = st.checkbox("Run regime detection (HMM if available)", value=False, key="adv_regime_on")
    if reg_on:
        try:
            reg = self.analytics.detect_regimes(s_aligned, n_states=int(getattr(cfg, "regime_states", 3)))
            if reg and reg.get("success", False):
                states = reg.get("states")
                if isinstance(states, pd.Series) and not states.empty:
                    fig = self.visualizer.create_regime_chart(states, title=f"Regimes — {label}")
                    st.plotly_chart(fig, use_container_width=True)
                st.json({k: v for k, v in reg.items() if k not in ("states",)}, expanded=False)
            else:
                st.warning(reg.get("message", "Regime detection returned no result."))
        except Exception as e:
            st.warning(f"Regime detection failed: {e}")

def _icd_display_risk_analytics_fallback(self, cfg):
    import numpy as np
    import pandas as pd
    import streamlit as st

    st.markdown("### 🧮 Risk Analytics (Institutional)")

    returns_df = _icd__get_returns_df(self)
    bench_df = _icd__get_benchmark_df(self)
    if returns_df.empty:
        st.info("Load data from the sidebar to begin.")
        return


    # Simple correlation (renewed)
    st.markdown("#### 🔗 Simple Asset Correlations (Renewed)")

    corr_kind = st.selectbox(
        "Correlation type",
        options=["pearson", "spearman", "kendall"],
        index=0,
        key="risk_corr_simple_kind"
    )
    min_obs = st.slider(
        "Minimum overlapping observations (min_periods)",
        min_value=10,
        max_value=300,
        value=60,
        step=5,
        key="risk_corr_simple_minobs"
    )

    corr_assets = [c for c in returns_df.columns if c is not None]
    corr_src = returns_df[corr_assets].copy()
    corr_src = corr_src.replace([np.inf, -np.inf], np.nan)

    # pandas DataFrame.corr is pairwise by default; min_periods prevents spurious ±1 from tiny overlap
    corr_df = corr_src.corr(method=str(corr_kind), min_periods=int(min_obs))

    # Keep diagonal at 1.0 (when assets exist)
    try:
        np.fill_diagonal(corr_df.values, 1.0)
    except Exception:
        pass

    if corr_df is not None and not corr_df.empty and corr_df.shape[0] >= 2:
        fig = self.visualizer.create_correlation_matrix(
            corr_df,
            title=f"Simple Correlations ({corr_kind}) — min_obs={int(min_obs)}"
        )
        st.plotly_chart(fig, use_container_width=True)

        with st.expander("Show overlap counts (N) used per pair", expanded=False):
            try:
                valid = corr_src.notna().astype(int)
                n_ij = valid.T.dot(valid)
                st.dataframe(n_ij)
            except Exception as _e:
                st.warning(f"Could not compute overlap counts: {_e}")
    else:
        st.warning("Correlation matrix unavailable (need at least 2 assets with sufficient overlap).")


    st.markdown("#### VaR / CVaR / ES (Historical • Parametric • Modified)")
    scope = st.radio(
        "Scope",
        options=["Equal-Weight Portfolio", "Single Asset"],
        index=0,
        horizontal=True,
        key="risk_scope"
    )

    if scope.startswith("Equal"):
        assets = list(returns_df.columns)
        default_assets = assets[: min(6, len(assets))]
        sel = st.multiselect("Assets (equal weight)", assets, default=default_assets, key="risk_assets")
        series = _icd__equal_weight_portfolio(returns_df, sel)
        label = "EW Portfolio"
    else:
        sym = st.selectbox("Select Asset", options=list(returns_df.columns), index=0, key="risk_asset")
        series = pd.to_numeric(returns_df[sym], errors="coerce").dropna()
        label = sym

    if series.empty or len(series) < 120:
        st.warning("Insufficient data for VaR (need ~120+ observations).")
        return

    c1, c2, c3 = st.columns(3)
    with c1:
        cl = st.select_slider("Confidence", options=[0.90, 0.95, 0.99], value=0.95, key="risk_cl")
    with c2:
        method = st.selectbox("Method", options=["historical", "parametric", "modified"], index=0, key="risk_var_method")
    with c3:
        horizon = st.select_slider("Horizon (days)", options=[1, 5, 10, 20], value=1, key="risk_horizon")

    try:
        out = self.analytics.calculate_var(series, confidence_level=float(cl), method=method, horizon=int(horizon)) or {}
    except Exception:
        out = {}

    if out and out.get("success", True):
        var = out.get("VaR")
        cvar = out.get("CVaR")
        es = out.get("ES", out.get("CVaR"))
        def _num(x):
            try:
                v = float(x)
                return v if np.isfinite(v) else np.nan
            except Exception:
                return np.nan
        var_s = _num(var)
        cvar_s = _num(cvar)
        es_s = _num(es)
        m1, m2, m3 = st.columns(3)
        def _fmt(v):
            try:
                v = float(v)
                return f"{v:.2%}" if np.isfinite(v) else "—"
            except Exception:
                return "—"
        with m1:
            st.metric(f"VaR {int(cl*100)}% ({horizon}d)", _fmt(var_s))
        with m2:
            st.metric(f"CVaR {int(cl*100)}% ({horizon}d)", _fmt(cvar_s))
        with m3:
            st.metric(f"ES {int(cl*100)}% ({horizon}d)", _fmt(es_s))

        if any([not np.isfinite(var_s), not np.isfinite(cvar_s), not np.isfinite(es_s)]):
            st.warning("VaR/CVaR/ES returned non-finite values. This usually means your effective sample is too small after cleaning/alignment. Try selecting fewer assets, increasing history, or lowering min overlap.")
        st.json({k: v for k, v in out.items() if k not in ("returns",)}, expanded=False)
    else:
        st.warning(out.get("message", "VaR engine returned no result."))

def _icd_display_ewma_ratio_signal_fallback(self, cfg):
    import numpy as np
    import pandas as pd
    import streamlit as st

    st.markdown("### 🚦 Institutional Signal — EWMA Vol Ratio (22 / (33 + 99))")

    returns_df = _icd__get_returns_df(self)
    if returns_df.empty:
        st.info("Load data from the sidebar to begin.")
        return

    scope = st.radio(
        "Scope",
        options=["Equal-Weight Portfolio", "Single Asset"],
        index=0,
        horizontal=True,
        key="sig_scope"
    )

    if scope.startswith("Equal"):
        assets = list(returns_df.columns)
        default_assets = assets[: min(6, len(assets))]
        sel = st.multiselect("Assets (equal weight)", assets, default=default_assets, key="sig_assets")
        series = _icd__equal_weight_portfolio(returns_df, sel)
        label = "EW Portfolio"
    else:
        sym = st.selectbox("Select Asset", options=list(returns_df.columns), index=0, key="sig_asset")
        series = pd.to_numeric(returns_df[sym], errors="coerce").dropna()
        label = sym

    if series.empty or len(series) < 120:
        st.warning("Insufficient data for signal (need ~120+ observations).")
        return

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        span_fast = st.number_input("Fast span", min_value=5, max_value=120, value=22, step=1, key="sig_span_fast")
    with c2:
        span_mid = st.number_input("Mid span", min_value=5, max_value=240, value=33, step=1, key="sig_span_mid")
    with c3:
        span_slow = st.number_input("Slow span", min_value=10, max_value=500, value=99, step=1, key="sig_span_slow")
    with c4:
        annualize = st.checkbox("Annualize vols", value=False, key="sig_annualize")

    bb1, bb2, bb3, bb4 = st.columns(4)
    with bb1:
        bb_window = st.number_input("BB window", min_value=5, max_value=120, value=20, step=1, key="sig_bb_window")
    with bb2:
        bb_k = st.number_input("BB k", min_value=0.5, max_value=5.0, value=2.0, step=0.1, key="sig_bb_k")
    with bb3:
        green_max = st.number_input("Green max", min_value=0.0, max_value=5.0, value=0.35, step=0.01, key="sig_green")
    with bb4:
        red_min = st.number_input("Red min", min_value=0.0, max_value=5.0, value=0.55, step=0.01, key="sig_red")

    ewma_df = self.analytics.compute_ewma_volatility_ratio(
        series,
        span_fast=int(span_fast),
        span_mid=int(span_mid),
        span_slow=int(span_slow),
        annualize=bool(annualize),
    )

    if ewma_df is None or ewma_df.empty:
        st.warning("Signal computation returned no result.")
        return

    fig = self.visualizer.create_ewma_ratio_signal_chart(
        ewma_df,
        title=f"EWMA Vol Ratio Signal — {label}",
        bb_window=int(bb_window),
        bb_k=float(bb_k),
        green_max=float(green_max),
        red_min=float(red_min),
        show_bollinger=True,
        show_threshold_lines=True,
    )
    st.plotly_chart(fig, use_container_width=True)

    latest = float(ewma_df["EWMA_RATIO"].iloc[-1])
    if latest <= float(green_max):
        band = "GREEN"
    elif latest < float(red_min):
        band = "ORANGE"
    else:
        band = "RED"

    c1, c2 = st.columns(2)
    with c1:
        st.metric("Latest Ratio", f"{latest:.4f}")
    with c2:
        st.metric("Risk Band", band)

    with st.expander("Method Notes", expanded=False):
        st.markdown(
            """
- Ratio = EWMA_VOL(FAST) / (EWMA_VOL(MID) + EWMA_VOL(SLOW))
- Bollinger Bands are computed on the ratio using rolling mean ± k·std.
- Colored zones reflect institutional thresholds (green/orange/red).
            """
        )

def _icd_display_portfolio_fallback(self, cfg):
    import numpy as np
    import pandas as pd
    import streamlit as st

    st.markdown("### 📈 Portfolio (Equal-Weight + Optimizer)")

    returns_df = _icd__get_returns_df(self)
    if returns_df.empty:
        st.info("Load data from the sidebar to begin.")
        return

    assets = list(returns_df.columns)
    default_assets = assets[: min(8, len(assets))]
    sel = st.multiselect("Portfolio Assets", assets, default=default_assets, key="port_assets")
    if not sel:
        st.warning("Select at least one asset.")
        return

    port = _icd__equal_weight_portfolio(returns_df, sel)
    if port.empty or len(port) < 120:
        st.warning("Insufficient portfolio history (need ~120+ observations).")
        return

    # Performance chart
    fig = self.visualizer.create_performance_chart(returns_df[sel], title="Asset Cumulative Performance")
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("#### Optimizer (Internal Engine)")
    method = st.selectbox("Optimization Method", options=["sharpe", "min_var", "max_ret"], index=0, key="port_opt_method")
    target = st.number_input("Target annual return (optional)", min_value=0.0, max_value=2.0, value=0.0, step=0.01, key="port_target")
    target_val = None if target <= 0 else float(target)

    try:
        out = self.analytics.optimize_portfolio(returns_df[sel], method=method, target_return=target_val)
    except Exception as e:
        out = {"success": False, "message": str(e)}

    if out.get("success", False):
        w = out.get("weights", {})
        if isinstance(w, dict) and w:
            w_df = pd.DataFrame({"Weight": w}).sort_values("Weight", ascending=False)
            st.dataframe(w_df.style.format({"Weight":"{:.2%}"}), use_container_width=True)
        st.json({k: v for k, v in out.items() if k not in ("weights",)}, expanded=False)
    else:
        st.warning(out.get("message", "Optimizer did not return a solution."))

def _icd_display_rolling_beta_fallback(self, cfg):
    import numpy as np
    import pandas as pd
    import streamlit as st
    import plotly.graph_objects as go

    st.markdown("### β Rolling Beta (vs Benchmark)")

    returns_df = _icd__get_returns_df(self)
    bench_df = _icd__get_benchmark_df(self)

    if returns_df.empty:
        st.info("Load data from the sidebar to begin.")
        return

    bench_series, bench_col = _icd__pick_benchmark_series(bench_df)
    if bench_series.empty:
        st.warning("No benchmark returns available. Select a benchmark and reload data.")
        return

    scope = st.radio(
        "Scope",
        options=["Equal-Weight Portfolio", "Single Asset"],
        index=0,
        horizontal=True,
        key="beta_scope"
    )
    if scope.startswith("Equal"):
        assets = list(returns_df.columns)
        default_assets = assets[: min(6, len(assets))]
        sel = st.multiselect("Assets (equal weight)", assets, default=default_assets, key="beta_assets")
        series = _icd__equal_weight_portfolio(returns_df, sel)
        label = "EW Portfolio"
    else:
        sym = st.selectbox("Select Asset", options=list(returns_df.columns), index=0, key="beta_asset")
        series = pd.to_numeric(returns_df[sym], errors="coerce").dropna()
        label = sym

    # align
    idx = series.index.intersection(bench_series.index)
    if len(idx) < 120:
        st.warning("Insufficient overlap with benchmark (need ~120+ observations).")
        return

    series = series.loc[idx]
    bench = bench_series.loc[idx]

    window = st.select_slider("Rolling window (days)", options=[20, 40, 60, 90, 120, 180, 252], value=int(getattr(cfg, "rolling_window", 60)), key="beta_window")

    # rolling beta via covariance
    cov = series.rolling(window).cov(bench)
    var = bench.rolling(window).var()
    beta = (cov / var).replace([np.inf, -np.inf], np.nan).dropna()

    if beta.empty:
        st.warning("Beta could not be computed (check data).")
        return

    latest = float(beta.iloc[-1])
    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Latest Beta", f"{latest:.3f}")
    with c2:
        st.metric("Benchmark", bench_col or "Benchmark")
    with c3:
        st.metric("Window", f"{window}d")

    # Bands: typical interpretation bands around 1.0
    green_lo, green_hi = 0.8, 1.2
    orange_lo, orange_hi = 0.6, 1.4

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=beta.index, y=beta.values, name="Rolling Beta", mode="lines"))
    # shaded zones
    x0, x1 = beta.index.min(), beta.index.max()
    fig.add_shape(type="rect", xref="x", yref="y", x0=x0, x1=x1, y0=green_lo, y1=green_hi,
                  fillcolor="rgba(34,197,94,0.12)", line=dict(width=0))
    fig.add_shape(type="rect", xref="x", yref="y", x0=x0, x1=x1, y0=orange_lo, y1=green_lo,
                  fillcolor="rgba(245,158,11,0.10)", line=dict(width=0))
    fig.add_shape(type="rect", xref="x", yref="y", x0=x0, x1=x1, y0=green_hi, y1=orange_hi,
                  fillcolor="rgba(245,158,11,0.10)", line=dict(width=0))
    fig.add_shape(type="rect", xref="x", yref="y", x0=x0, x1=x1, y0=orange_hi, y1=max(float(beta.max())*1.1, orange_hi+0.2),
                  fillcolor="rgba(239,68,68,0.08)", line=dict(width=0))
    fig.add_shape(type="rect", xref="x", yref="y", x0=x0, x1=x1, y0=min(float(beta.min())*0.9, orange_lo-0.2), y1=orange_lo,
                  fillcolor="rgba(239,68,68,0.08)", line=dict(width=0))

    fig.update_layout(title=f"Rolling Beta — {label} vs {bench_col}", height=520)
    st.plotly_chart(fig, use_container_width=True)

def _icd_display_stress_testing_fallback(self, cfg):
    import numpy as np
    import pandas as pd
    import streamlit as st

    st.markdown("### 🧯 Stress Testing (Scenario Shocks)")

    returns_df = _icd__get_returns_df(self)
    if returns_df.empty:
        st.info("Load data from the sidebar to begin.")
        return

    scope = st.radio("Scope", options=["Equal-Weight Portfolio", "Single Asset"], index=0, horizontal=True, key="st_scope")
    if scope.startswith("Equal"):
        assets = list(returns_df.columns)
        default_assets = assets[: min(6, len(assets))]
        sel = st.multiselect("Assets (equal weight)", assets, default=default_assets, key="st_assets")
        series = _icd__equal_weight_portfolio(returns_df, sel)
        label = "EW Portfolio"
    else:
        sym = st.selectbox("Select Asset", options=list(returns_df.columns), index=0, key="st_asset")
        series = pd.to_numeric(returns_df[sym], errors="coerce").dropna()
        label = sym

    if series.empty or len(series) < 120:
        st.warning("Insufficient history for stress testing (need ~120+ observations).")
        return

    shock = st.select_slider("Shock (return)", options=[-0.30, -0.20, -0.15, -0.10, -0.05, 0.05, 0.10], value=-0.10, key="st_shock")
    duration = st.select_slider("Shock duration (days)", options=[1, 5, 10, 20], value=5, key="st_duration")

    try:
        out = self.analytics.stress_test(series, shock=float(shock), duration=int(duration))
    except Exception as e:
        out = {"success": False, "message": str(e)}

    if out and out.get("success", False):
        st.success(f"Stress test computed for {label}.")
        st.json({k: v for k, v in out.items() if k not in ("path",)}, expanded=False)
        path = out.get("path")
        if isinstance(path, pd.Series) and not path.empty:
            import plotly.graph_objects as go
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=path.index, y=path.values, mode="lines", name="Simulated Path"))
            fig.update_layout(title=f"Stress Path — {label}", height=420)
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning(out.get("message", "Stress test returned no result."))

def _icd_display_reporting_fallback(self, cfg):
    import pandas as pd
    import streamlit as st
    import numpy as np
    from io import BytesIO

    st.markdown("### 🧾 Reporting (Exports)")

    returns_df = _icd__get_returns_df(self)
    bench_df = _icd__get_benchmark_df(self)
    if returns_df.empty:
        st.info("Load data from the sidebar to begin.")
        return

    bench_series, bench_col = _icd__pick_benchmark_series(bench_df)

    # Performance table for all assets
    rows = []
    for col in returns_df.columns:
        s = pd.to_numeric(returns_df[col], errors="coerce").dropna()
        bm = None
        if not bench_series.empty:
            idx = s.index.intersection(bench_series.index)
            if len(idx) >= 60:
                bm = bench_series.loc[idx]
                s_use = s.loc[idx]
            else:
                s_use = s
        else:
            s_use = s
        try:
            m = self.analytics.calculate_performance_metrics(s_use, benchmark_returns=bm)
        except Exception:
            m = {}
        if m:
            m["asset"] = col
            rows.append(m)

    if rows:
        df = pd.DataFrame(rows).set_index("asset")
        st.dataframe(df, use_container_width=True)

        # CSV download
        csv = df.to_csv().encode("utf-8")
        st.download_button("⬇️ Download Metrics CSV", data=csv, file_name="performance_metrics.csv", mime="text/csv", key="rep_csv")

        # Excel download
        bio = BytesIO()
        writer_obj, engine_used = icd_safe_excel_writer(bio)
        if writer_obj is None:
            st.error("Excel export disabled: install `openpyxl` or `xlsxwriter` in requirements.txt.")
        else:
            with writer_obj as writer:
                df.to_excel(writer, sheet_name="metrics")
        st.download_button("⬇️ Download Metrics Excel", data=bio.getvalue(), file_name="performance_metrics.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="rep_xlsx")
    else:
        st.warning("No metrics computed (data too short?).")

    st.markdown("#### QuantStats Report (optional)")
    qs_on = st.checkbox("Generate QuantStats HTML report (if installed)", value=False, key="rep_qs_on")
    if qs_on:
        try:
            import quantstats as qs
            # Choose scope
            scope = st.radio("QS Scope", options=["Equal-Weight Portfolio", "Single Asset"], index=0, horizontal=True, key="rep_qs_scope")
            if scope.startswith("Equal"):
                assets = list(returns_df.columns)
                default_assets = assets[: min(6, len(assets))]
                sel = st.multiselect("Assets (equal weight)", assets, default=default_assets, key="rep_qs_assets")
                series = _icd__equal_weight_portfolio(returns_df, sel)
                label = "EW Portfolio"
            else:
                sym = st.selectbox("Select Asset", options=list(returns_df.columns), index=0, key="rep_qs_asset")
                series = pd.to_numeric(returns_df[sym], errors="coerce").dropna()
                label = sym

            if series.empty or len(series) < 120:
                st.warning("Insufficient data for QuantStats report.")
                return

            html = qs.reports.html(series, output=None, title=f"QuantStats Report — {label}", download_filename=None)
            st.download_button("⬇️ Download QuantStats HTML", data=html.encode("utf-8"), file_name=f"quantstats_{label}.html",
                               mime="text/html", key="rep_qs_dl")
            st.success("Report generated.")
        except Exception as e:
            st.warning(f"QuantStats not available or failed: {e}")

def _icd_display_settings_fallback(self, cfg):
    import streamlit as st

    st.markdown("### ⚙️ Settings & Diagnostics")

    c1, c2 = st.columns(2)
    with c1:
        st.write("**Core Parameters**")
        cfg.risk_free_rate = st.number_input("Risk-free rate", min_value=0.0, max_value=0.2, value=float(getattr(cfg, "risk_free_rate", 0.02)),
                                             step=0.001, format="%.3f", key="set_rf")
        cfg.rolling_window = st.number_input("Default rolling window", min_value=10, max_value=500, value=int(getattr(cfg, "rolling_window", 60)),
                                             step=1, key="set_rollw")
        cfg.backtest_window = st.number_input("Backtest window", min_value=50, max_value=2000, value=int(getattr(cfg, "backtest_window", 250)),
                                              step=10, key="set_btwin")
    with c2:
        st.write("**Correlation Policy**")
        st.selectbox(
            "Default correlation method",
            options=["sample_aligned", "sample_pairwise", "ledoit_wolf"],
            index=0,
            key="set_corr_method",
            help="Used by Risk Analytics tab; Ledoit-Wolf requires scikit-learn."
        )
        st.checkbox("Default PSD enforcement", value=True, key="set_corr_psd")

    st.write("**Institutional Band Policies**")
    st.caption("Defaults: Tracking Error <4% green, 4–8% orange, >8% red. EWMA Ratio green<=0.35, red>=0.55.")
    st.number_input("Tracking Error green threshold", min_value=0.0, max_value=0.5, value=0.04, step=0.005, key="set_te_green")
    st.number_input("Tracking Error orange threshold", min_value=0.0, max_value=0.8, value=0.08, step=0.005, key="set_te_orange")

    st.markdown("#### Dependency Status")
    deps = {
        "arch (GARCH)": "arch",
        "hmmlearn (HMM regimes)": "hmmlearn",
        "scikit-learn (Ledoit–Wolf)": "sklearn",
        "PyPortfolioOpt": "pypfopt",
        "QuantStats": "quantstats",
    }
    status = {}
    for label, mod in deps.items():
        try:
            __import__(mod)
            status[label] = "✅ available"
        except Exception:
            status[label] = "⚠️ missing"
    st.json(status, expanded=False)

def _icd_display_portfolio_lab_fallback(self, cfg):
    import numpy as np
    import pandas as pd
    import streamlit as st

    st.markdown("### 🧰 Portfolio Lab (PyPortfolioOpt • Optional)")
    returns_df = _icd__get_returns_df(self)
    if returns_df.empty:
        st.info("Load data from the sidebar to begin.")
        return

    assets = list(returns_df.columns)
    default_assets = assets[: min(10, len(assets))]
    sel = st.multiselect("Select assets for optimization", assets, default=default_assets, key="pypf_assets")
    if len(sel) < 2:
        st.warning("Select at least 2 assets.")
        return

    df = returns_df[sel].dropna(how="any")
    if df.shape[0] < 120:
        st.warning("Need at least ~120 aligned observations for optimization.")
        return

    st.info("If PyPortfolioOpt is not installed in your environment, this tab will auto-fallback to the internal optimizer.")
    use_pypfopt = st.checkbox("Use PyPortfolioOpt (if available)", value=True, key="pypf_use")

    if use_pypfopt:
        try:
            from pypfopt import expected_returns, risk_models
            from pypfopt.efficient_frontier import EfficientFrontier
            from pypfopt import CLA
            from pypfopt.hierarchical_portfolio import HRPOpt

            mu = expected_returns.mean_historical_return(df, frequency=int(getattr(cfg, "annual_trading_days", 252)))
            S = risk_models.sample_cov(df, frequency=int(getattr(cfg, "annual_trading_days", 252)))

            opt_type = st.selectbox(
                "Optimizer",
                options=["Max Sharpe", "Min Volatility", "Efficient Risk", "Efficient Return", "CLA (min vol)", "HRP"],
                index=0,
                key="pypf_type"
            )

            if opt_type == "HRP":
                hrp = HRPOpt(df)
                w = hrp.optimize()
                w_df = pd.DataFrame({"Weight": w}).sort_values("Weight", ascending=False)
                st.dataframe(w_df.style.format({"Weight":"{:.2%}"}), use_container_width=True)
                perf = hrp.portfolio_performance(verbose=False)
                st.json({"expected_return": perf[0], "volatility": perf[1], "sharpe": perf[2]}, expanded=False)
                return

            if opt_type.startswith("CLA"):
                cla = CLA(mu, S)
                w = cla.min_volatility()
                w_df = pd.DataFrame({"Weight": w}).sort_values("Weight", ascending=False)
                st.dataframe(w_df.style.format({"Weight":"{:.2%}"}), use_container_width=True)
                perf = cla.portfolio_performance(verbose=False)
                st.json({"expected_return": perf[0], "volatility": perf[1], "sharpe": perf[2]}, expanded=False)
                return

            ef = EfficientFrontier(mu, S)

            if opt_type == "Max Sharpe":
                ef.max_sharpe(risk_free_rate=float(getattr(cfg, "risk_free_rate", 0.02)))
            elif opt_type == "Min Volatility":
                ef.min_volatility()
            elif opt_type == "Efficient Risk":
                target_risk = st.slider("Target risk (annual vol)", min_value=0.05, max_value=0.80, value=0.20, step=0.01, key="pypf_trisk")
                ef.efficient_risk(target_volatility=float(target_risk))
            elif opt_type == "Efficient Return":
                target_ret = st.slider("Target return (annual)", min_value=-0.10, max_value=1.00, value=0.15, step=0.01, key="pypf_tret")
                ef.efficient_return(target_return=float(target_ret))

            w = ef.clean_weights()
            w_df = pd.DataFrame({"Weight": w}).sort_values("Weight", ascending=False)
            st.dataframe(w_df.style.format({"Weight":"{:.2%}"}), use_container_width=True)

            perf = ef.portfolio_performance(verbose=False, risk_free_rate=float(getattr(cfg, "risk_free_rate", 0.02)))
            st.json({"expected_return": perf[0], "volatility": perf[1], "sharpe": perf[2]}, expanded=False)

        except Exception as e:
            st.warning(f"PyPortfolioOpt unavailable or failed ({e}). Falling back to internal optimizer.")
            use_pypfopt = False

    if not use_pypfopt:
        method = st.selectbox("Internal optimizer method", options=["sharpe", "min_var", "max_ret"], index=0, key="pypf_int_method")
        try:
            out = self.analytics.optimize_portfolio(df, method=method)
        except Exception as e:
            out = {"success": False, "message": str(e)}
        if out.get("success", False):
            w = out.get("weights", {})
            w_df = pd.DataFrame({"Weight": w}).sort_values("Weight", ascending=False)
            st.dataframe(w_df.style.format({"Weight":"{:.2%}"}), use_container_width=True)
        else:
            st.warning(out.get("message", "Internal optimizer failed."))

def run_scientific_platform_v7_2_ultra():
    """Wrapper for merged v7.2 Ultra platform."""
    try:
        platform = ScientificCommoditiesPlatform()
        platform.render()
    except Exception as e:
        import streamlit as st
        st.error(f"Scientific v7.2 platform failed to start: {e}")
        st.exception(e)

# Bind missing UI methods safely (no AttributeErrors)
try:
    InstitutionalCommoditiesDashboard  # noqa: F401
    _bind = {
        "_display_advanced_analytics": _icd_display_advanced_analytics_fallback,
        "_display_risk_analytics": _icd_display_risk_analytics_fallback,
        "_display_ewma_ratio_signal": _icd_display_ewma_ratio_signal_fallback,
        "_display_portfolio": _icd_display_portfolio_fallback,
        "_display_rolling_beta": _icd_display_rolling_beta_fallback,
        "_display_stress_testing": _icd_display_stress_testing_fallback,
        "_display_reporting": _icd_display_reporting_fallback,
        "_display_settings": _icd_display_settings_fallback,
        "_display_portfolio_lab": _icd_display_portfolio_lab_fallback,
    }
    for name, fn in _bind.items():
        if not hasattr(InstitutionalCommoditiesDashboard, name):
            setattr(InstitutionalCommoditiesDashboard, name, fn)
except Exception:
    pass



# =============================================================================
# USDJPY FZ Reaction Monitor (Michaelis–Menten + Peak-Down) — merged page
# Source integrated from user-provided script (keys namespaced to avoid collisions)
# =============================================================================

def run_usdjpy_fz_reaction_monitor():
    """USDJPY FZ Reaction Monitor — Michaelis–Menten + Peak-Down (merged page)."""
    # app.py
    """
    USDJPY FZ Reaction Monitor — Hybrid Model
    ========================================
    Hybrid "macro" model for USDJPY:
      1) Michaelis–Menten (single-substrate) saturating reaction-speed model
      2) Peak-Down process: downside spike hazard + expected jump-loss

    "Substrate driver" is built from an explicit FZ (Flow Zone) definition:

    FZ Definition (exact, algorithmic)
    ----------------------------------
    For each bar t:
      - Compute rolling high/low over a lookback window W:
          H_t = rolling_max(High, W)
          L_t = rolling_min(Low,  W)
          R_t = H_t - L_t
      - Define Flow Zone bounds using Fibonacci retracement band inside that range:
          FZ_low_t  = L_t + fib_low  * R_t - atr_mult * ATR_t
          FZ_high_t = L_t + fib_high * R_t + atr_mult * ATR_t
        where fib_low < fib_high (defaults: 0.382 and 0.618), and ATR is EWMA ATR.

    Interpretation:
      - FZ is a dynamic "fair-value / flow" band inside the latest range, expanded by ATR buffer.
      - Reactions are expected near the FZ edges.

    Substrate a_t
    -------------
    If Close_t is inside FZ:
      - edge_pressure_t = 1 - (min(distance to lower edge, distance to upper edge) / half_width)
        => edge_pressure = 1 at edges, 0 at center
    Else:
      - edge_pressure = 0

    Then:
      - speed_t = |log_return_t| / EWMA_vol_t
      - a_t = clip(edge_pressure_t * speed_t, 0, a_clip_max)

    Dashboard
    ---------
    - Live chart (Plotly): price + FZ band
    - Risk band + alerts
    - Signals table + CSV export
    - Calibration info

    Disclaimer: educational/research tool, not financial advice.
    """


    import time
    import warnings
    from dataclasses import dataclass
    from typing import Optional, Tuple, Dict, Any

    import numpy as np
    import pandas as pd
    import streamlit as st
    import plotly.graph_objects as go

    try:
        import yfinance as yf
    except Exception as e:
        st.error("Missing dependency: yfinance. Add it to requirements.txt.")
        st.stop()

    try:
        from scipy.optimize import curve_fit
    except Exception as e:
        st.error("Missing dependency: scipy. Add it to requirements.txt.")
        st.stop()

    # Optional: auto-refresh
    try:
        from streamlit_autorefresh import st_autorefresh
        _HAS_AUTOREFRESH = True
    except Exception:
        _HAS_AUTOREFRESH = False

    # Optional: statsmodels for logistic fit
    try:
        import statsmodels.api as sm
        _HAS_STATSMODELS = True
    except Exception:
        _HAS_STATSMODELS = False


    # -----------------------------------------------------------------------------
    # App setup
    # -----------------------------------------------------------------------------

    warnings.filterwarnings("ignore")


    # -----------------------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------------------
    def sigmoid(x: np.ndarray) -> np.ndarray:
        x = np.clip(x, -50, 50)
        return 1.0 / (1.0 + np.exp(-x))


    def mm_rate(a: np.ndarray, V: float, Km: float) -> np.ndarray:
        return (V * a) / (Km + a + 1e-12)


    def ewma_vol(r: pd.Series, span: int) -> pd.Series:
        return r.ewm(span=span, adjust=False, min_periods=span).std()


    def compute_atr(df: pd.DataFrame, span: int) -> pd.Series:
        high = df["High"].astype(float)
        low = df["Low"].astype(float)
        close = df["Close"].astype(float)
        prev_close = close.shift(1)
        tr = pd.concat([(high - low).abs(), (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
        return tr.ewm(span=span, adjust=False, min_periods=span).mean()


    def rsi(close: pd.Series, n: int) -> pd.Series:
        delta = close.diff()
        up = delta.clip(lower=0.0)
        down = (-delta).clip(lower=0.0)
        roll_up = up.ewm(alpha=1/n, adjust=False, min_periods=n).mean()
        roll_down = down.ewm(alpha=1/n, adjust=False, min_periods=n).mean()
        rs = roll_up / (roll_down + 1e-12)
        return 100 - (100 / (1 + rs))


    def _fix_yf_columns(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [c[0] for c in df.columns]
        df = df.rename(columns={c: c.title() for c in df.columns})
        df.index = pd.to_datetime(df.index)
        return df


    @st.cache_data(ttl=60, show_spinner=False)
    def fetch_usdjpy(interval: str, period: str, primary: str = "USDJPY=X", fallback: str = "JPY=X") -> pd.DataFrame:
        """
        Yahoo Finance FX volume may be NaN; this is OK.
        For intraday intervals, Yahoo restricts max period (e.g., 15m ~ 60d).
        """
        def _dl(ticker: str) -> pd.DataFrame:
            df = yf.download(ticker, period=period, interval=interval, auto_adjust=False, progress=False, threads=True)
            df = _fix_yf_columns(df)
            if not df.empty:
                df["Ticker"] = ticker
            return df

        df = _dl(primary)
        if df.empty:
            df = _dl(fallback)
        if df.empty:
            raise RuntimeError("Yahoo Finance returned empty data for USDJPY.")
        # Ensure required columns
        for c in ["Open", "High", "Low", "Close"]:
            if c not in df.columns:
                raise RuntimeError(f"Missing {c} in Yahoo data columns: {list(df.columns)}")
        return df


    # -----------------------------------------------------------------------------
    # Hybrid model (FZ-driven)
    # -----------------------------------------------------------------------------
    @dataclass
    class FZConfig:
        lookback: int = 96
        fib_low: float = 0.382
        fib_high: float = 0.618
        atr_span: int = 14
        atr_mult: float = 0.25


    @dataclass
    class ModelConfig:
        vol_span: int = 20
        rsi_window: int = 14
        ema_fast: int = 12
        ema_slow: int = 48

        horizon_bars: int = 6
        jump_quantile: float = 0.995
        min_train: int = 400

        a_clip: Tuple[float, float] = (0.0, 10.0)


    @dataclass
    class HybridParams:
        # Michaelis–Menten
        V: float
        Km: float

        # Peak-down logistic weights
        w0: float
        w_upper: float
        w_rsi: float
        w_vol: float
        w_trend: float

        # Jump magnitude
        jump_mean: float

        # Jump intensity
        lam0: float
        lam1: float


    def compute_fz(df: pd.DataFrame, fz: FZConfig) -> pd.DataFrame:
        out = df.copy()
        atr = compute_atr(out, span=fz.atr_span)
        roll_high = out["High"].astype(float).rolling(fz.lookback, min_periods=fz.lookback).max()
        roll_low = out["Low"].astype(float).rolling(fz.lookback, min_periods=fz.lookback).min()
        rng = (roll_high - roll_low).clip(lower=1e-12)

        fz_low = roll_low + fz.fib_low * rng - fz.atr_mult * atr
        fz_high = roll_low + fz.fib_high * rng + fz.atr_mult * atr

        # safety: ensure low <= high
        swap = fz_low > fz_high
        if swap.any():
            tmp = fz_low.copy()
            fz_low = fz_low.where(~swap, fz_high)
            fz_high = fz_high.where(~swap, tmp)

        out["ATR"] = atr
        out["FZ_low"] = fz_low
        out["FZ_high"] = fz_high
        out["FZ_center"] = (fz_low + fz_high) / 2.0
        out["FZ_halfw"] = (fz_high - fz_low) / 2.0
        out["in_FZ"] = (out["Close"].astype(float) >= fz_low) & (out["Close"].astype(float) <= fz_high)
        return out


    def build_features(df: pd.DataFrame, fz_cfg: FZConfig, mcfg: ModelConfig) -> pd.DataFrame:
        out = compute_fz(df, fz_cfg)
        close = out["Close"].astype(float)
        logp = np.log(close.replace(0, np.nan))
        r = logp.diff()

        vol = ewma_vol(r, span=mcfg.vol_span)
        rsi_v = rsi(close, n=mcfg.rsi_window)

        ema_fast = close.ewm(span=mcfg.ema_fast, adjust=False, min_periods=mcfg.ema_slow).mean()
        ema_slow = close.ewm(span=mcfg.ema_slow, adjust=False, min_periods=mcfg.ema_slow).mean()
        trend = (ema_fast - ema_slow) / (ema_slow + 1e-12)

        # Edge pressure inside FZ: 1 at edges, 0 at center
        halfw = out["FZ_halfw"].astype(float).replace(0, np.nan)
        dist_to_low = (close - out["FZ_low"].astype(float)).abs()
        dist_to_high = (out["FZ_high"].astype(float) - close).abs()
        dist_edge = np.minimum(dist_to_low, dist_to_high)
        edge_pressure = (1.0 - (dist_edge / (halfw + 1e-12))).clip(0.0, 1.0)
        edge_pressure = edge_pressure.where(out["in_FZ"], 0.0)

        # Upper-edge pressure (0..1) used for peak-down (downside) bias
        z = ((close - out["FZ_center"].astype(float)) / (halfw + 1e-12)).clip(-1.0, 1.0)  # -1..1
        upper_edge = np.maximum(z, 0.0)  # 0..1 when above center
        upper_edge = pd.Series(upper_edge, index=out.index).where(out["in_FZ"], 0.0)

        speed = (r.abs() / (vol + 1e-12)).clip(0.0, mcfg.a_clip[1])
        a = (edge_pressure * speed).clip(mcfg.a_clip[0], mcfg.a_clip[1])

        out["logp"] = logp
        out["r"] = r
        out["vol"] = vol
        out["rsi"] = rsi_v
        out["trend"] = trend
        out["edge_pressure"] = edge_pressure
        out["upper_edge"] = upper_edge
        out["speed"] = speed
        out["a"] = a

        out["fwd_r"] = out["r"].shift(-mcfg.horizon_bars)
        return out


    def fit_hybrid(feats: pd.DataFrame, mcfg: ModelConfig) -> HybridParams:
        df = feats.dropna(subset=["r", "vol", "a", "upper_edge", "rsi", "trend", "fwd_r"]).copy()
        if len(df) < mcfg.min_train:
            raise ValueError(f"Not enough data to fit. Have {len(df)}, need at least {mcfg.min_train} bars.")

        # 1) Fit Michaelis–Menten on |r| vs a
        a = df["a"].to_numpy(float)
        y = df["r"].abs().to_numpy(float)

        V0 = float(np.nanpercentile(y, 95)) if np.isfinite(np.nanpercentile(y, 95)) else 1e-3
        Km0 = float(np.nanmedian(a[a > 0])) if np.any(a > 0) else 0.5

        bounds = ([1e-12, 1e-6], [np.inf, np.inf])
        try:
            (V_hat, Km_hat), _ = curve_fit(mm_rate, a, y, p0=[V0, Km0], bounds=bounds, maxfev=20000)
        except Exception:
            V_hat = float(np.nanpercentile(y, 99))
            Km_hat = float(np.nanmedian(a[a > 0])) if np.any(a > 0) else 0.5

        # 2) Define down-jump threshold from tail of |r|
        r = df["r"].to_numpy(float)
        thr = float(np.nanquantile(np.abs(r), mcfg.jump_quantile))
        is_jump_down = (r < -thr).astype(int)

        down_tail = -r[r < -thr]  # positive magnitudes
        jump_mean = float(np.nanmean(down_tail)) if len(down_tail) > 5 else float(thr)

        # 3) Peak-down events: price in FZ and near upper edge + forward drop
        k = 2.0
        peak_down = ((df["upper_edge"] > 0.25) & (df["fwd_r"] < -k * df["vol"])).astype(int)

        # Logistic model on:
        #  - upper_edge
        #  - RSI overbought (normalized)
        #  - vol deviation
        #  - trend (negative trend increases peak-down probability)
        X = np.column_stack([
            np.ones(len(df)),
            df["upper_edge"].to_numpy(float),
            (df["rsi"].to_numpy(float) - 50.0) / 10.0,
            (df["vol"].to_numpy(float) / (df["vol"].median() + 1e-12)) - 1.0,
            df["trend"].to_numpy(float),
        ])
        y_pd = peak_down.to_numpy(int)

        w = np.array([-2.0, 3.5, 1.0, 0.8, -2.0], dtype=float)  # sensible defaults
        if _HAS_STATSMODELS:
            try:
                model = sm.Logit(y_pd, X)
                res = model.fit(disp=False, maxiter=200)
                w = res.params.astype(float)
            except Exception:
                pass

        # 4) Jump intensity λ ≈ lam0 + lam1 * a
        a_clip = np.clip(a, 0, mcfg.a_clip[1])
        yj = is_jump_down.astype(float)
        A = np.column_stack([np.ones_like(a_clip), a_clip])
        try:
            lam_hat, *_ = np.linalg.lstsq(A, yj, rcond=None)
            lam0, lam1 = float(max(lam_hat[0], 1e-8)), float(max(lam_hat[1], 0.0))
        except Exception:
            lam0, lam1 = 1e-4, 1e-3

        return HybridParams(
            V=float(V_hat), Km=float(Km_hat),
            w0=float(w[0]), w_upper=float(w[1]), w_rsi=float(w[2]), w_vol=float(w[3]), w_trend=float(w[4]),
            jump_mean=float(jump_mean),
            lam0=float(lam0), lam1=float(lam1),
        )


    def predict_hybrid(feats: pd.DataFrame, params: HybridParams, mcfg: ModelConfig) -> pd.DataFrame:
        df = feats.copy()

        a = df["a"].fillna(0.0).to_numpy(float)
        mm = mm_rate(a, params.V, params.Km)
        df["mm_speed"] = mm

        X = np.column_stack([
            np.ones(len(df)),
            df["upper_edge"].fillna(0.0).to_numpy(float),
            (df["rsi"].fillna(50.0).to_numpy(float) - 50.0) / 10.0,
            (df["vol"].fillna(df["vol"].median()).to_numpy(float) / (df["vol"].median() + 1e-12)) - 1.0,
            df["trend"].fillna(0.0).to_numpy(float),
        ])
        logits = X @ np.array([params.w0, params.w_upper, params.w_rsi, params.w_vol, params.w_trend], dtype=float)
        p_peakdown = sigmoid(logits)
        df["p_peakdown"] = p_peakdown

        lam = np.clip(params.lam0 + params.lam1 * np.clip(a, 0, mcfg.a_clip[1]), 0, 0.5)
        df["lambda_down"] = lam
        p_jump = 1.0 - np.exp(-lam)
        df["p_jump_down"] = p_jump

        # Direction gate: within FZ, if close is above center (upper_edge>0) bias to downside; else trend-follow.
        trend_sign = np.sign(df["trend"].fillna(0.0).to_numpy(float))
        in_fz = df["in_FZ"].fillna(False).to_numpy(bool)
        upper = df["upper_edge"].fillna(0.0).to_numpy(float) > 0.35
        sign = np.where(in_fz & upper, -1.0, np.where(trend_sign == 0, 1.0, trend_sign))
        df["dir"] = sign

        exp_jump_loss = p_peakdown * p_jump * params.jump_mean
        df["exp_jump_loss"] = exp_jump_loss
        df["mu_hat"] = sign * mm - exp_jump_loss

        # Composite downside risk score
        score = 100.0 * np.clip(0.65 * p_peakdown + 0.35 * p_jump, 0.0, 1.0)
        df["risk_score"] = score
        return df


    # -----------------------------------------------------------------------------
    # Plotting
    # -----------------------------------------------------------------------------
    def make_price_chart(df: pd.DataFrame, show_markers: bool = True) -> go.Figure:
        fig = go.Figure()

        fig.add_trace(go.Candlestick(
            x=df.index,
            open=df["Open"], high=df["High"], low=df["Low"], close=df["Close"],
            name="USDJPY",
            increasing_line_width=1,
            decreasing_line_width=1,
        ))

        # FZ band
        if "FZ_low" in df.columns and "FZ_high" in df.columns:
            fig.add_trace(go.Scatter(
                x=df.index, y=df["FZ_high"],
                mode="lines", line=dict(width=1),
                name="FZ High",
            ))
            fig.add_trace(go.Scatter(
                x=df.index, y=df["FZ_low"],
                mode="lines", line=dict(width=1),
                fill="tonexty",
                name="FZ Low (band)",
                opacity=0.15,
            ))

        if show_markers and "risk_score" in df.columns:
            # Mark red/orange signals on close
            latest_n = min(300, len(df))
            sub = df.tail(latest_n).copy()
            red = sub[sub["risk_score"] >= 75]
            org = sub[(sub["risk_score"] >= 50) & (sub["risk_score"] < 75)]
            if not red.empty:
                fig.add_trace(go.Scatter(
                    x=red.index, y=red["Close"],
                    mode="markers", name="High Risk",
                    marker=dict(size=7, symbol="triangle-down"),
                ))
            if not org.empty:
                fig.add_trace(go.Scatter(
                    x=org.index, y=org["Close"],
                    mode="markers", name="Medium Risk",
                    marker=dict(size=6, symbol="circle"),
                    opacity=0.7,
                ))

        fig.update_layout(
            height=560,
            margin=dict(l=10, r=10, t=30, b=10),
            xaxis_title="Time",
            yaxis_title="USDJPY",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        return fig


    def make_risk_chart(df: pd.DataFrame) -> go.Figure:
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df.index, y=df["risk_score"], mode="lines", name="Risk Score (0-100)"))
        # Horizontal bands
        fig.add_hrect(y0=0, y1=50, opacity=0.08, line_width=0)
        fig.add_hrect(y0=50, y1=75, opacity=0.12, line_width=0)
        fig.add_hrect(y0=75, y1=100, opacity=0.16, line_width=0)

        fig.update_layout(
            height=260,
            margin=dict(l=10, r=10, t=30, b=10),
            yaxis=dict(range=[0, 100], title="Risk"),
            xaxis_title="Time",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        return fig


    def make_prob_chart(df: pd.DataFrame) -> go.Figure:
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df.index, y=df["p_peakdown"], mode="lines", name="P(peak-down)"))
        fig.add_trace(go.Scatter(x=df.index, y=df["p_jump_down"], mode="lines", name="P(down-jump)"))
        fig.update_layout(
            height=260,
            margin=dict(l=10, r=10, t=30, b=10),
            yaxis=dict(range=[0, 1], title="Probability"),
            xaxis_title="Time",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        return fig


    # -----------------------------------------------------------------------------
    # Alerts
    # -----------------------------------------------------------------------------
    def render_alerts(latest: pd.Series) -> None:
        risk = float(latest.get("risk_score", np.nan))
        p_pd = float(latest.get("p_peakdown", np.nan))
        p_jd = float(latest.get("p_jump_down", np.nan))
        a = float(latest.get("a", np.nan))

        if not np.isfinite(risk):
            return

        if risk >= 85 and p_pd >= 0.60:
            st.error(f"🚨 HIGH ALERT: Downside reaction risk is EXTREME (risk={risk:.1f}, Ppeakdown={p_pd:.2f}, Pjump={p_jd:.2f}, a={a:.2f})")
        elif risk >= 75:
            st.warning(f"⚠️ Warning: Downside reaction risk is HIGH (risk={risk:.1f}, Ppeakdown={p_pd:.2f}, Pjump={p_jd:.2f}, a={a:.2f})")
        elif risk >= 50:
            st.info(f"🟠 Watch: Medium risk (risk={risk:.1f}, Ppeakdown={p_pd:.2f}, Pjump={p_jd:.2f}, a={a:.2f})")
        else:
            st.success(f"🟢 Normal: Low risk (risk={risk:.1f}, Ppeakdown={p_pd:.2f}, Pjump={p_jd:.2f}, a={a:.2f})")


    # -----------------------------------------------------------------------------
    # Sidebar controls
    # -----------------------------------------------------------------------------
    with st.sidebar:
        st.title("⚙️ Controls")

        colA, colB = st.columns(2)
        with colA:
            interval = st.selectbox("Interval", ["15m", "30m", "1h", "2h", "4h", "1d"], index=2, key="usdjpy_interval")
        with colB:
            # Suggest compatible periods
            period = st.selectbox("Period", ["60d", "180d", "365d", "730d", "max"], index=3, key="usdjpy_period")

        st.divider()
        st.subheader("FZ Definition (exact)")
        lookback = st.slider("FZ lookback (bars)", 48, 300, 96, 6, key="usdjpy_fz_lookback")
        fib_low = st.number_input("fib_low", min_value=0.05, max_value=0.49, value=0.382, step=0.001, format="%.3f", key="usdjpy_fib_low")
        fib_high = st.number_input("fib_high", min_value=0.51, max_value=0.95, value=0.618, step=0.001, format="%.3f", key="usdjpy_fib_high")
        atr_span = st.slider("ATR span", 5, 50, 14, 1, key="usdjpy_atr_span")
        atr_mult = st.slider("ATR buffer multiplier", 0.0, 2.0, 0.25, 0.05, key="usdjpy_atr_mult")

        st.divider()
        st.subheader("Model")
        vol_span = st.slider("EWMA vol span", 10, 80, 20, 1, key="usdjpy_vol_span")
        rsi_window = st.slider("RSI window", 7, 30, 14, 1, key="usdjpy_rsi_window")
        ema_fast = st.slider("EMA fast", 4, 30, 12, 1, key="usdjpy_ema_fast")
        ema_slow = st.slider("EMA slow", 20, 120, 48, 1, key="usdjpy_ema_slow")

        horizon_bars = st.slider("Peak-down horizon (bars)", 1, 24, 6, 1, key="usdjpy_horizon_bars")
        jump_q = st.slider("Jump quantile", 0.950, 0.999, 0.995, 0.001, key="usdjpy_jump_q")
        a_clip_max = st.slider("Substrate clip max", 2.0, 20.0, 10.0, 0.5, key="usdjpy_a_clip_max")

        st.divider()
        st.subheader("Live / Alerts")
        use_autorefresh = st.checkbox("Auto-refresh", value=False, key="usdjpy_autorefresh")
        refresh_sec = st.slider("Refresh (seconds)", 10, 300, 30, 5, key="usdjpy_refresh_sec")
        last_n_chart = st.slider("Bars on charts", 150, 1200, 450, 50, key="usdjpy_last_n_chart")

        st.caption("Tip: intraday intervals often require short periods (e.g., 15m with 60d).")


    # Auto-refresh
    if use_autorefresh and _HAS_AUTOREFRESH:
        st_autorefresh(interval=int(refresh_sec * 1000), limit=None, key="usdjpy_auto_rerun")
    elif use_autorefresh and not _HAS_AUTOREFRESH:
        st.info("Auto-refresh needs streamlit-autorefresh. Add it to requirements.txt (already included in the provided file).")


    # -----------------------------------------------------------------------------
    # Main run
    # -----------------------------------------------------------------------------
    st.title("📈 USDJPY FZ Reaction Monitor — Michaelis–Menten + Peak-Down")

    with st.expander("Model summary (what you are monitoring)", expanded=False):
        st.markdown(
            """
    - **FZ (Flow Zone)** is computed from a rolling range (**High/Low lookback**) and a **fib retracement band**, expanded by an **ATR buffer**.
    - **Substrate a(t)** rises when price is **inside the FZ and close to the edges**, while price is moving fast (vol-adjusted).
    - **Michaelis–Menten** saturates the reaction speed: higher a(t) increases expected move magnitude, but with diminishing returns.
    - **Peak-down** module estimates downside reaction probability near the **upper side of the FZ**, plus tail-jump risk.
            """
        )

    # Fetch and compute
    with st.spinner("Fetching USDJPY and computing signals..."):
        raw = fetch_usdjpy(interval=interval, period=period)
        fz_cfg = FZConfig(lookback=int(lookback), fib_low=float(fib_low), fib_high=float(fib_high), atr_span=int(atr_span), atr_mult=float(atr_mult))
        mcfg = ModelConfig(
            vol_span=int(vol_span), rsi_window=int(rsi_window),
            ema_fast=int(ema_fast), ema_slow=int(ema_slow),
            horizon_bars=int(horizon_bars), jump_quantile=float(jump_q),
            a_clip=(0.0, float(a_clip_max)),
        )
        feats = build_features(raw, fz_cfg, mcfg)

        # Fit only on sufficiently clean region; show fit diagnostics
        try:
            params = fit_hybrid(feats, mcfg)
        except Exception as e:
            st.error(f"Model fit failed: {e}")
            st.stop()

        scored = predict_hybrid(feats, params, mcfg)

    # Slice for charts
    plot_df = scored.dropna(subset=["Close"]).tail(int(last_n_chart)).copy()

    # KPIs
    latest = scored.dropna(subset=["mu_hat", "risk_score"]).iloc[-1]

    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("Close", f"{latest['Close']:.4f}")
    k2.metric("In FZ", "Yes" if bool(latest.get("in_FZ", False)) else "No")
    k3.metric("Substrate a(t)", f"{latest.get('a', np.nan):.2f}")
    k4.metric("MM speed", f"{latest.get('mm_speed', np.nan):.6f}")
    k5.metric("μ̂ (next-bar)", f"{latest.get('mu_hat', np.nan):.6f}")
    k6.metric("Risk score", f"{latest.get('risk_score', np.nan):.1f}")

    render_alerts(latest)

    # Layout: charts + signal panels
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Price & FZ", "🧠 Signals", "🧪 Calibration", "📋 Data"])

    with tab1:
        st.plotly_chart(make_price_chart(plot_df, show_markers=True), use_container_width=True)
        st.plotly_chart(make_risk_chart(plot_df.dropna(subset=["risk_score"])), use_container_width=True)

    with tab2:
        c1, c2 = st.columns([2, 1])
        with c1:
            st.plotly_chart(make_prob_chart(plot_df.dropna(subset=["p_peakdown", "p_jump_down"])), use_container_width=True)
        with c2:
            st.subheader("Latest signal breakdown")
            st.write({
                "timestamp": str(latest.name),
                "ticker": str(raw["Ticker"].iloc[-1]),
                "in_FZ": bool(latest.get("in_FZ", False)),
                "FZ_low": float(latest.get("FZ_low", np.nan)),
                "FZ_high": float(latest.get("FZ_high", np.nan)),
                "edge_pressure": float(latest.get("edge_pressure", np.nan)),
                "upper_edge": float(latest.get("upper_edge", np.nan)),
                "a": float(latest.get("a", np.nan)),
                "mm_speed": float(latest.get("mm_speed", np.nan)),
                "p_peakdown": float(latest.get("p_peakdown", np.nan)),
                "p_jump_down": float(latest.get("p_jump_down", np.nan)),
                "exp_jump_loss": float(latest.get("exp_jump_loss", np.nan)),
                "mu_hat": float(latest.get("mu_hat", np.nan)),
                "risk_score": float(latest.get("risk_score", np.nan)),
            })

        st.divider()
        st.subheader("Band zones (green/orange/red)")
        st.markdown(
            """
    - **Green:** risk < 50  
    - **Orange:** 50 ≤ risk < 75  
    - **Red:** risk ≥ 75  
    You can tighten/loosen these by adjusting the model thresholds (risk chart + markers are based on these cutoffs).
            """
        )

    with tab3:
        st.subheader("Fitted parameters")
        st.code(
            f"V={params.V:.6g}, Km={params.Km:.6g}\n"
            f"Logit weights: w0={params.w0:.4f}, w_upper={params.w_upper:.4f}, w_rsi={params.w_rsi:.4f}, "
            f"w_vol={params.w_vol:.4f}, w_trend={params.w_trend:.4f}\n"
            f"Jump mean (down tail)={params.jump_mean:.6g}\n"
            f"Jump intensity: lam0={params.lam0:.6g}, lam1={params.lam1:.6g}",
            language="text"
        )

        st.subheader("Quick sanity plots (last 500 bars)")
        tmp = scored.dropna(subset=["a", "mm_speed", "r"]).tail(500).copy()
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=tmp.index, y=tmp["a"], mode="lines", name="a(t) substrate"))
        fig.update_layout(height=250, margin=dict(l=10, r=10, t=30, b=10), yaxis_title="a(t)")
        st.plotly_chart(fig, use_container_width=True)

        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(x=tmp.index, y=tmp["r"].abs(), mode="lines", name="|r|"))
        fig2.add_trace(go.Scatter(x=tmp.index, y=tmp["mm_speed"], mode="lines", name="MM fitted speed"))
        fig2.update_layout(height=260, margin=dict(l=10, r=10, t=30, b=10), yaxis_title="log-return")
        st.plotly_chart(fig2, use_container_width=True)

        st.caption("If MM speed lags too much, increase vol span or adjust FZ buffer/edges so a(t) reacts more selectively.")

    with tab4:
        st.subheader("Signals table")
        show_cols = [
            "Open", "High", "Low", "Close",
            "FZ_low", "FZ_high", "in_FZ",
            "a", "mm_speed", "p_peakdown", "p_jump_down", "mu_hat", "risk_score",
        ]
        tbl = scored[show_cols].dropna(subset=["Close"]).tail(300).copy()
        st.dataframe(tbl, use_container_width=True, height=520)

        st.download_button(
            "Download CSV (last 300 rows)",
            data=tbl.to_csv(index=True).encode("utf-8"),
            file_name="usdjpy_fz_mm_peakdown_signals.csv",
            mime="text/csv",
            key="usdjpy_dl_csv",
        )

    st.divider()
    st.caption("Educational/research dashboard. Not financial advice.")

def _run_app_router():
    import streamlit as st

    st.sidebar.markdown("### 🧭 Platform Mode")
    mode = st.sidebar.radio(
        "Select application layer",
        options=[
            "🏛️ Institutional Commodities Platform (v6.x)",
            "🧪 Scientific Commodities Platform (v7.2 Ultra)",
            "🧠 Quantum Sovereign Terminal (v14.0)",
            "💱 USDJPY FZ Reaction Monitor (MM + Peak-Down)",
        ],
        index=0,
        key="app_mode_selector"
    )

    if mode == "🧠 Quantum Sovereign Terminal (v14.0)":
        run_quantum_sovereign_v14_terminal()
    elif mode == "🧪 Scientific Commodities Platform (v7.2 Ultra)":
        run_scientific_platform_v7_2_ultra()
    elif mode == "💱 USDJPY FZ Reaction Monitor (MM + Peak-Down)":
        run_usdjpy_fz_reaction_monitor()
    else:
        # Ensure InstitutionalCommoditiesDashboard exists and is runnable
        try:
            dashboard = InstitutionalCommoditiesDashboard()
            dashboard.run()
        except Exception as e:
            st.error(f"Institutional dashboard failed to start: {e}")
            st.exception(e)

# Execute router (Streamlit entrypoint)
_run_app_router()
