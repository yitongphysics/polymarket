import math
from typing import Optional, Tuple

import numpy as np
from scipy.stats import norm
from scipy.optimize import brentq
from scipy.interpolate import PchipInterpolator


def _safe_brentq(obj, lo=1e-6, hi=5.0):
    # Find a root of obj on [lo, hi], widening hi until a sign change or giving up.
    f_lo, f_hi = obj(lo), obj(hi)
    expands = 0
    while f_lo * f_hi > 0 and hi < 50.0 and expands < 6:
        hi *= 2.0
        f_hi = obj(hi)
        expands += 1
    if f_lo * f_hi > 0:
        return None
    return brentq(obj, lo, hi, maxiter=200, xtol=1e-12, rtol=1e-12)


def BS_call(F: float, K: float, sigma: float, T: float) -> float:
    # Black–Scholes call on a forward F with volatility sigma and time T.
    if T <= 0 or sigma <= 0:
        return max(F - K, 0.0)
    vol_sqrt_t = sigma * math.sqrt(T)
    d1 = (math.log(F / K) + 0.5 * sigma * sigma * T) / vol_sqrt_t
    d2 = d1 - vol_sqrt_t
    return F * norm.cdf(d1) - K * norm.cdf(d2)


def BS_put(F: float, K: float, sigma: float, T: float) -> float:
    # Black–Scholes put on a forward F with volatility sigma and time T.
    if T <= 0 or sigma <= 0:
        return max(K - F, 0.0)
    vol_sqrt_t = sigma * math.sqrt(T)
    d1 = (math.log(F / K) + 0.5 * sigma * sigma * T) / vol_sqrt_t
    d2 = d1 - vol_sqrt_t
    return K * norm.cdf(-d2) - F * norm.cdf(-d1)


def implied_prob_above_strike_auto(
    K: float,
    T: float,
    C: Optional[float] = None,
    F: Optional[float] = None,
    r: float = 0.0,
) -> Tuple:
    # Implied risk-neutral P(F_T > K) from a call price when T > 0, using Brent on implied vol.
    if T <= 0:
        return (1.0 if (F is not None and F > K) else 0.0), float("nan"), float("inf"), "na", (F if F is not None else float("nan"))

    prefer_call = K >= F

    def _bs_call(F, K, sigma, T):
        if sigma <= 0:
            return max(F - K, 0.0)
        vol_sqrt_t = sigma * math.sqrt(T)
        d1 = (math.log(F / K) + 0.5 * sigma * sigma * T) / vol_sqrt_t
        d2 = d1 - vol_sqrt_t
        return F * norm.cdf(d1) - K * norm.cdf(d2)

    def _solve_sigma_from_call():
        if C is None:
            return None
        obj = lambda s: _bs_call(F, K, s, T) - C
        return _safe_brentq(obj)

    sigma = None
    if prefer_call:
        sigma = _solve_sigma_from_call()

    if sigma is None:
        return None, None, None, None, None

    vol_sqrt_t = sigma * math.sqrt(T)
    d2 = (math.log(F / K) - 0.5 * sigma * sigma * T) / vol_sqrt_t
    prob = norm.cdf(d2)
    return prob, sigma, d2, "call", F


def prob_market_above_strike(K: float, F: float, T: float, IV: float):
    # Black–Scholes probability that terminal price exceeds strike given forward F and IV.
    try:
        d2 = (math.log(F / K) - 0.5 * IV * IV * T) / (IV * math.sqrt(T))
        return norm.cdf(d2)
    except Exception:
        return None


def monotone_cubic_interp(x, y, x_new, extrapolate=True):
    # PCHIP interpolation of (x, y) evaluated at x_new (scalar or array).
    p = PchipInterpolator(x, y, extrapolate=extrapolate)
    xq = np.asarray(x_new)
    yq = p(xq)
    return float(yq) if np.ndim(x_new) == 0 else yq


def derivative_call(Ks, Cs, K, dK=100):
    # Central finite-difference derivative dC/dK at K using monotone spline through (Ks, Cs).
    p = PchipInterpolator(Ks, Cs, extrapolate=True)
    C1, C2 = p(K + dK), p(K - dK)
    return (C1 - C2) / (2 * dK)


def compute_prob(Ks, Cs, K, dK=100, r=0.0, T=0.0):
    # Risk-neutral density contribution from call prices: -e^{-rT} dC/dK.
    DF = np.exp(-r * T)
    return -DF * derivative_call(Ks, Cs, K, dK)
