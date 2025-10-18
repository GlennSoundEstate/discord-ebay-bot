import os
import re
import pytz
import asyncio
import discord
import time
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from ebaysdk.trading import Connection as Trading

# Access them like this:
EBAY_AUTH_TOKEN = os.getenv("EBAY_AUTH_TOKEN")
EBAY_APP_ID = os.getenv("EBAY_APP_ID")
EBAY_DEV_ID = os.getenv("EBAY_DEV_ID")
EBAY_CERT_ID = os.getenv("EBAY_CERT_ID")
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")

OUTPUT_FILE = "active_best_offers_with_sku.xlsx"
CHANNEL_MAP_FILE = "discord_channel_map.xlsx"
MAX_PAGES = 600

def normalize_key(text):
    return re.sub(r'\W+', '', text.strip().lower())

def convert_utc_to_pacific(utc_string):
    try:
        utc_dt = datetime.strptime(utc_string, "%Y-%m-%dT%H:%M:%S.000Z")
        utc_dt = pytz.utc.localize(utc_dt)
        pacific_dt = utc_dt.astimezone(pytz.timezone("US/Pacific"))
        return pacific_dt.strftime("%Y-%m-%d %I:%M %p %Z")
    except Exception:
        return utc_string

def load_channel_map():
    channel_map = {}
    try:
        df = pd.read_excel(CHANNEL_MAP_FILE, dtype=str)
        for _, row in df.iterrows():
            sku = normalize_key(row.get("SKU", ""))
            channel_id = row.get("ChannelID", "").strip()
            if sku and channel_id:
                channel_map[sku] = int(channel_id)
    except Exception as e:
        print(f"⚠️ Failed to load channel map: {e}")
    return channel_map

class DiscordNotifier(discord.Client):
    def __init__(self, unnotified_df, channel_map, df, **kwargs):
        super().__init__(**kwargs)
        self.unnotified_df = unnotified_df
        self.channel_map = channel_map
        self.df = df

    async def on_ready(self):
        print(f"🤖 Logged in as {self.user}")
        self.df["ChannelNotified"] = self.df["ChannelNotified"].astype("string")

        for idx, offer in self.unnotified_df.iterrows():
            sku = normalize_key(offer.get("SKU", ""))
            channel_id = self.channel_map.get(sku)
            if not channel_id:
                print(f"🚫 No channel mapped for SKU {sku}")
                continue

            channel = self.get_channel(channel_id)
            if not channel:
                print(f"❌ Channel ID {channel_id} not found")
                continue

            message = "📢 You have a new offer on an eBay item.\nRun `/checkoffers` to get offer details."

            try:
                await channel.send(message)
                print(f"✅ Alert sent for SKU {sku} to channel {channel_id}")
                self.df.at[idx, "ChannelNotified"] = "Y"
            except Exception as e:
                print(f"❌ Failed to send alert for SKU {sku}: {e}")

        self.df.to_excel(OUTPUT_FILE, index=False)
        print(f"\n✅ Discord alerts sent. Updated {OUTPUT_FILE} with ChannelNotified flags.")

        await self.close()
        await self.http.close()

def run_offer_sync():
    print(f"\n⏰ Starting offer sync at {datetime.now().strftime('%Y-%m-%d %I:%M:%S %p')}")
    ebay = Trading(appid=EBAY_APP_ID, devid=EBAY_DEV_ID, certid=EBAY_CERT_ID, token=EBAY_AUTH_TOKEN, config_file=None)

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

        if not isinstance(data, dict) or data.get('Ack') not in ['Success', 'Warning']:
            print(f"⚠️ Unexpected or failed response on page {page}")
            page += 1
            continue

        item_best_offers_array = data.get('ItemBestOffersArray')
        if page == 1 and not item_best_offers_array:
            print("📭 No active best offers found. Exiting early.")
            break

        if not item_best_offers_array:
            print(f"📭 Page {page}: No ItemBestOffersArray found.")
            page += 1
            continue



        item_best_offers = item_best_offers_array.get('ItemBestOffers', [])
        items = [item_best_offers] if isinstance(item_best_offers, dict) else item_best_offers

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

    sku_map = {}
    for item_id in item_ids:
        try:
            item_response = ebay.execute('GetItem', {'ItemID': item_id, 'DetailLevel': 'ReturnAll'})
            sku = item_response.dict().get('Item', {}).get('SKU', '')
            sku_map[item_id] = sku
        except Exception:
            sku_map[item_id] = ''

    for offer in offers:
        offer["SKU"] = sku_map.get(offer["ItemID"], '')

    new_df = pd.DataFrame(offers)

    if new_df.empty:
        print("📭 No new offers to process. Skipping Excel update.")
        return



    new_df["ItemID"] = new_df["ItemID"].apply(lambda x: str(int(float(x))) if pd.notnull(x) else "")
    new_df["BestOfferID"] = new_df["BestOfferID"].astype(str)
    new_df["ProcessedToDiscord"] = None
    new_df["ChannelNotified"] = None

    if os.path.exists(OUTPUT_FILE):
        existing_df = pd.read_excel(OUTPUT_FILE, dtype={"BestOfferID": str})
        existing_ids = set(existing_df["BestOfferID"].dropna().astype(str))
        new_only_df = new_df[~new_df["BestOfferID"].isin(existing_ids)]
        combined_df = pd.concat([existing_df, new_only_df], ignore_index=True)
    else:
        combined_df = new_df

    if "ChannelNotified" not in combined_df.columns:
        combined_df["ChannelNotified"] = None

    combined_df.to_excel(OUTPUT_FILE, index=False)
    print(f"\n📁 Exported to: {OUTPUT_FILE} (existing records untouched)")

async def run_discord_alerts():
    df = pd.read_excel(OUTPUT_FILE, dtype={"BestOfferID": str})
    if "ChannelNotified" not in df.columns:
        df["ChannelNotified"] = None

    unnotified_df = df[df["ProcessedToDiscord"].isna() & df["ChannelNotified"].isna()]
    channel_map = load_channel_map()

    notifier = DiscordNotifier(unnotified_df, channel_map, df, intents=discord.Intents.default())
    await notifier.login(DISCORD_TOKEN)
    await notifier.connect()
    await notifier.close()           # ✅ Ensures graceful shutdown
    await notifier.http.close()      # ✅ Closes lingering aiohttp connector

# === Run every hour, crash-proof ===
while True:
    try:
        run_offer_sync()
        asyncio.run(run_discord_alerts())
    except Exception as e:
        print(f"💥 Unhandled error in sync loop: {e}")
    print("🕒 Sleeping for 1 hour...\n")
    time.sleep(60 * 60)



