from __future__ import annotations

import io
import json
from pathlib import Path

import pandas as pd
import requests


OUT_DIR = Path(__file__).resolve().parent
UNIVERSE_PATH = Path(r"C:\TradingData\eqidv2\fno_oi\universe\latest_near_month.parquet")
RETRIEVED_ON = "2026-10-21"


def url(path: str, *, archive: bool = False) -> str:
    if archive:
        return f"https://nsearchives.nseindia.com/content/indices/{path}"
    return f"https://www.niftyindices.com/IndexConstituent/{path}"


BROAD = {
    "Nifty 50": url("ind_nifty50list.csv", archive=True),
    "Nifty Next 50": url("ind_niftynext50list.csv", archive=True),
    "Nifty 100": url("ind_nifty100list.csv", archive=True),
    "Nifty Next 100": url("ind_niftyNext100_list.csv"),
    "Nifty 200": url("ind_nifty200list.csv", archive=True),
    "Nifty Total Market": url("ind_niftytotalmarket_list.csv", archive=True),
    "Nifty 500": url("ind_nifty500list.csv", archive=True),
    "Nifty500 Multicap 50:25:25": url("ind_nifty500Multicap502525_list.csv"),
    "Nifty500 LargeMidSmall Equal-Cap Weighted": url("ind_nifty500LargeMidSmallEqualCapWeighted_list.csv"),
    "Nifty Midcap 150": url("ind_niftymidcap150list.csv", archive=True),
    "Nifty Midcap 50": url("ind_niftymidcap50list.csv", archive=True),
    "Nifty Midcap Select": url("ind_niftymidcapselect_list.csv", archive=True),
    "Nifty Midcap 100": url("ind_niftymidcap100list.csv", archive=True),
    "Nifty Smallcap 500": url("ind_NiftySmallcap500_list.csv"),
    "Nifty Smallcap 250": url("ind_niftysmallcap250list.csv", archive=True),
    "Nifty Smallcap 50": url("ind_niftysmallcap50list.csv", archive=True),
    "Nifty Smallcap 100": url("ind_niftysmallcap100list.csv", archive=True),
    "Nifty Microcap 250": url("ind_niftymicrocap250_list.csv", archive=True),
    "Nifty LargeMidcap 250": url("ind_niftylargemidcap250list.csv", archive=True),
    "Nifty MidSmallcap 400": url("ind_niftymidsmallcap400list.csv", archive=True),
    "Nifty MidSmallcap400 50:50": url("ind_niftyMidSmallcap4005050_list.csv"),
    "Nifty India FPI 150": url("ind_niftyIndiaFPI150_list.csv"),
}

SECTORAL = {
    "Nifty Auto": url("ind_niftyautolist.csv"),
    "Nifty Bank": url("ind_niftybanklist.csv"),
    "Nifty Capital Goods": url("ind_niftyCapitalGoods_list.csv"),
    "Nifty Cement": url("ind_NiftyCement_list.csv"),
    "Nifty Chemicals": url("ind_niftyChemicals_list.csv"),
    "Nifty Commercial & Transport Services": url("ind_niftyCommercialTransportServices_list.csv"),
    "Nifty Construction": url("ind_niftyConstruction_list.csv"),
    "Nifty Consumer Durables": url("ind_niftyconsumerdurableslist.csv"),
    "Nifty Consumer Services": url("ind_niftyConsumerServices_list.csv"),
    "Nifty Financial Services": url("ind_niftyfinancelist.csv"),
    "Nifty Financial Services 25/50": url("ind_niftyfinancialservices25-50list.csv"),
    "Nifty Financial Services Ex-Bank": url("ind_niftyfinancialservicesexbank_list.csv"),
    "Nifty FMCG": url("ind_niftyfmcglist.csv"),
    "Nifty Healthcare": url("ind_niftyhealthcarelist.csv"),
    "Nifty Hospitals": url("ind_niftyHospitals_list.csv"),
    "Nifty Housing Finance": url("ind_niftyHousingFinance_list.csv"),
    "Nifty Insurance": url("ind_niftyInsurance_list.csv"),
    "Nifty IT": url("ind_niftyitlist.csv"),
    "Nifty Media": url("ind_niftymedialist.csv"),
    "Nifty Metal": url("ind_niftymetallist.csv"),
    "Nifty MidSmall Financial Services": url("ind_niftymidsmallfinancailservice_list.csv"),
    "Nifty MidSmall Healthcare": url("ind_niftymidsmallhealthcare_list.csv"),
    "Nifty MidSmall IT & Telecom": url("ind_niftymidsmallitAndtelecom_list.csv"),
    "Nifty NBFC": url("ind_niftyNBFC_list.csv"),
    "Nifty Oil & Gas": url("ind_niftyoilgaslist.csv"),
    "Nifty Pharma": url("ind_niftypharmalist.csv"),
    "Nifty Power": url("ind_niftyPower_list.csv"),
    "Nifty Private Bank": url("ind_nifty_privatebanklist.csv"),
    "Nifty PSU Bank": url("ind_niftypsubanklist.csv"),
    "Nifty Realty": url("ind_niftyrealtylist.csv"),
    "Nifty REITs & Realty": url("ind_niftyREITsRealty_list.csv"),
    "Nifty Retail": url("ind_niftyRetail_list.csv"),
    "Nifty Telecommunications": url("ind_niftyTelecommunications_list.csv"),
    "Nifty500 Healthcare": url("ind_nifty500Healthcare_list.csv"),
}

THEMATIC = {
    "Nifty Capital Markets": url("ind_niftyCapitalMarkets_list.csv"),
    "Nifty Commodities": url("ind_niftycommoditieslist.csv"),
    "Nifty Conglomerate 50": url("ind_niftyConglomerate50_list.csv"),
    "Nifty Core Housing": url("ind_niftyCoreHousing_list.csv"),
    "Nifty CPSE": url("ind_niftycpselist.csv"),
    "Nifty EV & New Age Automotive": url("ind_niftyEv_NewAgeAutomotive_list.csv"),
    "Nifty Energy": url("ind_niftyenergylist.csv"),
    "Nifty Housing": url("ind_niftyhousing_list.csv"),
    "Nifty India Consumption": url("ind_niftyconsumptionlist.csv"),
    "Nifty India Defence": url("ind_niftyindiadefence_list.csv"),
    "Nifty India Digital": url("ind_niftyindiadigital_list.csv"),
    "Nifty India Infrastructure & Logistics": url("ind_niftyIndiaInfrastructure_Logistics_list.csv"),
    "Nifty India Internet": url("ind_niftyIndiaInternet_list.csv"),
    "Nifty India Manufacturing": url("ind_niftyindiamanufacturing_list.csv"),
    "Nifty India New Age Consumption": url("ind_niftyIndiaNewAgeConsumption_list.csv"),
    "Nifty India Railways PSU": url("ind_niftyIndiaRailwaysPSU_list.csv"),
    "Nifty India Tourism": url("ind_niftyindiatourism_list.csv"),
    "Nifty Infrastructure": url("ind_niftyinfralist.csv"),
    "Nifty IPO": url("ind_niftyIPO_list.csv"),
    "Nifty Midcap Liquid 15": url("ind_Nifty_Midcap_Liquid15.csv"),
    "Nifty MNC": url("ind_niftymnclist.csv"),
    "Nifty Mobility": url("ind_niftymobility_list.csv"),
    "Nifty PSE": url("ind_niftypselist.csv"),
    "Nifty REITs & InvITs": url("ind_niftyREITs_InvITs_list.csv"),
    "Nifty Rural": url("ind_niftyRural_list.csv"),
    "Nifty Non-Cyclical Consumer": url("ind_niftynon-cyclicalconsumer_list.csv"),
    "Nifty Services Sector": url("ind_niftyservicelist.csv"),
    "Nifty100 Liquid 15": url("ind_Nifty100_Liquid15.csv"),
    "Nifty SME Emerge": url("ind_niftysmelist.csv"),
    "Nifty Waves": url("ind_niftyWaves_list.csv"),
    "Nifty India Corporate Group - Aditya Birla Group": url("ind_nifty_adityabirlalist.csv"),
    "Nifty India Corporate Group - Mahindra Group": url("ind_nifty_mahindralist.csv"),
    "Nifty India Corporate Group - Tata Group": url("ind_nifty_tatalist.csv"),
    "Nifty India Select 5 Corporate Groups (MAATR)": url("ind_niftyIndiaSelect5CorporateGroupsMAATR_list.csv"),
}

STRATEGY = {
    "Nifty100 Equal Weight": url("ind_nifty100list.csv"),
    "Nifty100 Low Volatility 30": url("ind_Nifty100LowVolatility30list.csv"),
    "Nifty200 Momentum 30": url("ind_nifty200Momentum30_list.csv"),
    "Nifty200 Alpha 30": url("ind_nifty200alpha30_list.csv"),
    "Nifty100 Alpha 30": url("ind_nifty100Alpha30list.csv"),
    "Nifty Dividend Opportunities 50": url("ind_niftydivopp50list.csv"),
    "Nifty Growth Sectors 15": url("ind_NiftyGrowth_Sectors15_Index.csv"),
    "Nifty High Beta 50": url("nifty_High_Beta50_Index.csv"),
    "Nifty Low Volatility 50": url("nifty_low_Volatility50_Index.csv"),
    "Nifty Top 10 Equal Weight": url("ind_niftytop10EqualWeight_list.csv"),
    "Nifty Top 15 Equal Weight": url("ind_niftytop15EqualWeight_list.csv"),
    "Nifty Top 20 Equal Weight": url("ind_niftyTop20EqualWeight_list.csv"),
    "Nifty100 Quality 30": url("ind_nifty100Quality30list.csv"),
    "Nifty Midcap150 Momentum 50": url("ind_niftymidcap150momentum50_list.csv"),
    "Nifty Midcap150 Quality 50": url("ind_niftymidcap150quality50list.csv"),
    "Nifty Alpha 50": url("ind_nifty_Alpha_Index.csv"),
    "Nifty50 Equal Weight": url("ind_Nifty50EqualWeight.csv"),
    "Nifty50 Value 20": url("ind_Nifty50_Value20.csv"),
    "Nifty200 Value 30": url("ind_nifty200Value30_list.csv"),
    "Nifty500 Flexicap Quality 30": url("ind_nifty500FlexicapQuality30_list.csv"),
    "Nifty500 Value 50": url("ind_nifty500Value50_list.csv"),
    "Nifty500 Equal Weight": url("ind_nifty500EqualWeight_list.csv"),
    "Nifty500 Low Volatility 50": url("ind_nifty500LowVolatility50_list.csv"),
    "Nifty500 Momentum 50": url("ind_nifty500Momentum50_list.csv"),
    "Nifty500 Quality 50": url("ind_nifty500Quality50_list.csv"),
    "Nifty500 Multifactor MQVLv 50": url("ind_nifty500MultifactorMQVLv50_list.csv"),
    "Nifty200 Quality 30": url("ind_nifty200Quality30_list.csv"),
    "Nifty MidSmallcap400 Momentum Quality 100": url("ind_niftyMidSmallcap400MomentumQuality100_list.csv"),
    "Nifty Smallcap250 Quality 50": url("ind_niftySmallcap250_Quality50_list.csv"),
    "Nifty Total Market Momentum Quality 50": url("ind_niftyTotalMarketMomentumQuality50_list.csv"),
}


def clean_columns(frame: pd.DataFrame) -> pd.DataFrame:
    frame.columns = [str(c).strip().replace("\ufeff", "") for c in frame.columns]
    return frame


def fetch_csv(source_url: str) -> pd.DataFrame:
    response = requests.get(
        source_url,
        timeout=30,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; one-time index constituent audit)",
            "Accept": "text/csv,text/plain,application/octet-stream,*/*",
        },
    )
    response.raise_for_status()
    frame = clean_columns(pd.read_csv(io.BytesIO(response.content)))
    if "Symbol" not in frame.columns:
        raise ValueError(f"missing Symbol column; got {list(frame.columns)}")
    frame["Symbol"] = frame["Symbol"].astype(str).str.strip()
    return frame


def main() -> None:
    universe = pd.read_parquet(UNIVERSE_PATH)
    equity = universe.loc[~universe["is_index_future"].astype(bool)].copy()
    equity["symbol"] = equity["equity_symbol"].astype(str).str.strip()
    symbols = sorted(equity["symbol"].unique().tolist())
    if len(symbols) != 213:
        raise RuntimeError(f"Expected 213 equity F&O symbols, found {len(symbols)}")

    families = [
        ("Broad", BROAD, "Complete current public catalogue"),
        ("Sectoral", SECTORAL, "Complete current public catalogue"),
        ("Thematic", THEMATIC, "Selected public constituent files"),
        ("Strategy", STRATEGY, "Selected public constituent files"),
    ]

    memberships: dict[str, dict[str, list[str]]] = {
        symbol: {family: [] for family, _, _ in families} for symbol in symbols
    }
    metadata: dict[str, dict[str, str]] = {symbol: {"company": "", "isin": ""} for symbol in symbols}
    details: list[dict[str, str]] = []
    sources: list[dict[str, object]] = []

    for family, manifest, scope in families:
        for index_name, source_url in manifest.items():
            try:
                frame = fetch_csv(source_url)
                fno_frame = frame[frame["Symbol"].isin(symbols)].copy()
                for _, row in fno_frame.iterrows():
                    symbol = str(row["Symbol"]).strip()
                    memberships[symbol][family].append(index_name)
                    company = str(row.get("Company Name", "")).strip()
                    isin = str(row.get("ISIN Code", "")).strip()
                    if company and company.lower() != "nan" and not metadata[symbol]["company"]:
                        metadata[symbol]["company"] = company
                    if isin and isin.lower() != "nan" and not metadata[symbol]["isin"]:
                        metadata[symbol]["isin"] = isin
                    details.append(
                        {
                            "retrieved_on": RETRIEVED_ON,
                            "symbol": symbol,
                            "company": metadata[symbol]["company"] or company,
                            "isin": metadata[symbol]["isin"] or isin,
                            "index": index_name,
                            "family": family,
                            "source_url": source_url,
                        }
                    )
                sources.append(
                    {
                        "family": family,
                        "index": index_name,
                        "scope": scope,
                        "status": "Fetched",
                        "constituents": int(len(frame)),
                        "fno_overlap": int(len(fno_frame)),
                        "source_url": source_url,
                        "retrieved_on": RETRIEVED_ON,
                        "error": "",
                    }
                )
            except Exception as exc:
                sources.append(
                    {
                        "family": family,
                        "index": index_name,
                        "scope": scope,
                        "status": "Unavailable",
                        "constituents": 0,
                        "fno_overlap": 0,
                        "source_url": source_url,
                        "retrieved_on": RETRIEVED_ON,
                        "error": str(exc)[:300],
                    }
                )

    primary_order = [
        "Nifty 50",
        "Nifty Next 50",
        "Nifty Midcap 150",
        "Nifty Smallcap 250",
        "Nifty Microcap 250",
    ]
    rows: list[dict[str, object]] = []
    for symbol in symbols:
        by_family = memberships[symbol]
        for values in by_family.values():
            values.sort()
        primary = next((name for name in primary_order if name in by_family["Broad"]), "Unmapped")
        all_indices = []
        for family in ("Broad", "Sectoral", "Thematic", "Strategy"):
            all_indices.extend(by_family[family])
        rows.append(
            {
                "symbol": symbol,
                "company": metadata[symbol]["company"],
                "isin": metadata[symbol]["isin"],
                "primary_size_index": primary,
                "broad_indices": "; ".join(by_family["Broad"]),
                "sectoral_indices": "; ".join(by_family["Sectoral"]),
                "thematic_indices": "; ".join(by_family["Thematic"]),
                "strategy_indices": "; ".join(by_family["Strategy"]),
                "all_mapped_indices": "; ".join(all_indices),
                "index_count": len(all_indices),
                "mapping_status": "Broad + specialised" if any(by_family[f] for f in ("Sectoral", "Thematic", "Strategy")) else "Broad only",
            }
        )

    details.sort(key=lambda row: (row["symbol"], row["family"], row["index"]))
    sources.sort(key=lambda row: (str(row["family"]), str(row["index"])))
    primary_counts = {name: sum(1 for row in rows if row["primary_size_index"] == name) for name in primary_order}
    summary = {
        "retrieved_on": RETRIEVED_ON,
        "stock_count": len(rows),
        "primary_mapped": sum(1 for row in rows if row["primary_size_index"] != "Unmapped"),
        "primary_counts": primary_counts,
        "specialised_mapped": sum(1 for row in rows if row["mapping_status"] == "Broad + specialised"),
        "source_count": len(sources),
        "sources_fetched": sum(1 for row in sources if row["status"] == "Fetched"),
        "sources_unavailable": sum(1 for row in sources if row["status"] != "Fetched"),
        "membership_rows": len(details),
    }
    payload = {"summary": summary, "stocks": rows, "memberships": details, "sources": sources}
    (OUT_DIR / "mapping_data.json").write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    unavailable = [row for row in sources if row["status"] != "Fetched"]
    if unavailable:
        print("Unavailable sources:")
        for row in unavailable:
            print(f"- {row['family']} | {row['index']} | {row['error']}")


if __name__ == "__main__":
    main()
