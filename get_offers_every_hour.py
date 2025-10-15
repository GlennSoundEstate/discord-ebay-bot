import pandas as pd
from datetime import datetime
import pytz
import os
import time
from ebaysdk.trading import Connection as Trading

load_dotenv()  # Loads variables from .env

# Access them like this:
EBAY_AUTH_TOKEN = os.getenv("EBAY_AUTH_TOKEN")
EBAY_APP_ID = os.getenv("EBAY_APP_ID")
EBAY_DEV_ID = os.getenv("EBAY_DEV_ID")
EBAY_CERT_ID = os.getenv("EBAY_CERT_ID")
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")


OUTPUT_FILE = "active_best_offers_with_sku.xlsx"
MAX_PAGES = 600

def convert_utc_to_pacific(utc_string):
    try:
        utc_dt = datetime.strptime(utc_string, "%Y-%m-%dT%H:%M:%S.000Z")
        utc_dt = pytz.utc.localize(utc_dt)
        pacific_dt = utc_dt.astimezone(pytz.timezone("US/Pacific"))
        return pacific_dt.strftime("%Y-%m-%d %I:%M %p %Z")
    except Exception:
        return utc_string

def run_offer_sync():
    print(f"\n⏰ Starting offer sync at {datetime.now().strftime('%Y-%m-%d %I:%M:%S %p')}")

    ebay = Trading(
        appid=EBAY_APP_ID,
        devid=EBAY_DEV_ID,
        certid=EBAY_CERT_ID,
        token=EBAY_AUTH_TOKEN,
        config_file=None
    )

    offers = []
    item_ids = set()
    page = 1
    total_pages = None

    while page <= MAX_PAGES:
        try:
            response = ebay.execute('GetBestOffers', {
                'DetailLevel': 'ReturnAll',
                'BestOfferStatus': 'All',
                'Pagination': {'EntriesPerPage': '200', 'PageNumber': str(page)}
            })
            data = response.dict()
        except Exception as e:
            print(f"❌ API call failed on page {page}: {e}")
            page += 1
            continue

        if not isinstance(data, dict):
            print(f"⚠️ Unexpected response type: {type(data)}")
            page += 1
            continue

        ack = data.get('Ack', 'No Ack')
        if ack not in ['Success', 'Warning']:
            print(f"❌ API Ack={ack} on page {page}")
            page += 1
            continue

        item_best_offers_array = data.get('ItemBestOffersArray')
        if not isinstance(item_best_offers_array, dict):
            print(f"📭 Page {page}: No ItemBestOffersArray found.")
            page += 1
            continue

        item_best_offers = item_best_offers_array.get('ItemBestOffers', [])
        items = [item_best_offers] if isinstance(item_best_offers, dict) else item_best_offers
        if not items:
            print(f"📭 Page {page}: No offers found.")
            page += 1
            continue

        print(f"📦 Page {page}: {len(items)} listings with offers")

        for item in items:
            item_info = item.get('Item', {})
            item_id = item_info.get('ItemID')
            title = item_info.get('Title', '')
            bin_price = float(item_info.get('BuyItNowPrice', {}).get('value', 0))
            bin_currency = item_info.get('BuyItNowPrice', {}).get('_currencyID', '???')

            item_ids.add(item_id)

            best_offer_array = item.get('BestOfferArray', {}).get('BestOffer', [])
            best_offers = [best_offer_array] if isinstance(best_offer_array, dict) else best_offer_array

            for offer in best_offers:
                if offer.get('BestOfferCodeType') == 'SellerCounterOffer':
                    continue
                if offer.get('Status') not in ['Pending', 'Expired']:
                    continue

                offers.append({
                    "FetchedOn": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "ItemID": item_id,
                    "Title": title,
                    "BinPrice": bin_price,
                    "BinCurrency": bin_currency,
                    "BestOfferID": str(offer.get("BestOfferID")),
                    "BuyerUserID": offer.get("Buyer", {}).get("UserID"),
                    "BuyerMessage": offer.get("BuyerMessage"),
                    "OfferAmount": float(offer.get("Price", {}).get("value", 0)),
                    "OfferCurrency": offer.get("Price", {}).get("_currencyID", '???'),
                    "Quantity": int(offer.get("Quantity", 1)),
                    "ExpirationTime (Pacific)": convert_utc_to_pacific(offer.get("ExpirationTime")),
                    "OfferType": offer.get("BestOfferCodeType"),
                    "Status": offer.get("Status")
                })

        if total_pages is None:
            total_pages = int(data.get('PaginationResult', {}).get('TotalNumberOfPages', 1))

        page += 1
        if page > total_pages:
            break

    print(f"\n🎯 Offers retained: {len(offers)}")

    # === Fetch SKUs ===
    sku_map = {}
    for item_id in item_ids:
        try:
            item_response = ebay.execute('GetItem', {
                'ItemID': item_id,
                'DetailLevel': 'ReturnAll'
            })
            sku = item_response.dict().get('Item', {}).get('SKU', '')
            sku_map[item_id] = sku
        except Exception:
            sku_map[item_id] = ''

    for offer in offers:
        offer["SKU"] = sku_map.get(offer["ItemID"], '')

    # === Export to Excel ===
    new_df = pd.DataFrame(offers)
    new_df["ItemID"] = new_df["ItemID"].apply(lambda x: str(int(float(x))) if pd.notnull(x) else "")
    new_df["BestOfferID"] = new_df["BestOfferID"].astype(str)

    if os.path.exists(OUTPUT_FILE):
        existing_df = pd.read_excel(OUTPUT_FILE, dtype={"BestOfferID": str})
        existing_ids = set(existing_df["BestOfferID"].dropna().astype(str))
        new_only_df = new_df[~new_df["BestOfferID"].isin(existing_ids)]

        if new_only_df.empty:
            print("✅ No new offers to append.")
            return

        combined_df = pd.concat([existing_df, new_only_df], ignore_index=True)
    else:
        combined_df = new_df

    combined_df.to_excel(OUTPUT_FILE, index=False)
    print(f"\n📁 Exported to: {OUTPUT_FILE} (existing records untouched)")

# === Run every hour, crash-proof ===
while True:
    try:
        run_offer_sync()
    except Exception as e:
        print(f"💥 Unhandled error in sync loop: {e}")
    print("🕒 Sleeping for 1 hour...\n")
    time.sleep(60 * 60)