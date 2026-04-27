"""CLI: download account activity to Parquet."""

import argparse
import os

from polymarket.api.account import download_account_activity


def main():
    parser = argparse.ArgumentParser(description="Download account activity")
    parser.add_argument("--wallet", required=True, help="Wallet address")
    parser.add_argument(
        "--limit",
        type=int,
        default=500,
        help="Total rows to fetch (auto-pages if >500)",
    )
    parser.add_argument(
        "--all-types",
        action="store_true",
        help="Include non-trade activity rows",
    )
    parser.add_argument(
        "-o",
        "--output",
        default=None,
        help="Parquet output path (default: apps/recording/data/wallet_<wallet>.parquet)",
    )
    args = parser.parse_args()

    if args.output:
        out_path = os.path.abspath(args.output)
    else:
        data_dir = os.path.join(os.path.dirname(__file__), "data")
        os.makedirs(data_dir, exist_ok=True)
        out_path = os.path.join(data_dir, f"wallet_{args.wallet}.parquet")

    out_dir = os.path.dirname(out_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    df = download_account_activity(
        wallet_address=args.wallet,
        total_limit=args.limit,
        trades_only=not args.all_types,
    )
    df.to_parquet(out_path, index=False)
    print(f"wrote {len(df)} rows to {out_path}")


if __name__ == "__main__":
    main()
