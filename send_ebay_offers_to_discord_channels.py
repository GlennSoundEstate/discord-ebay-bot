import discord
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime
from ebaysdk.trading import Connection as Trading
from discord.ext import commands
from discord import app_commands

# === Load Discord token ===
load_dotenv()
TOKEN = os.getenv("DISCORD_BOT_TOKEN")

# === File paths ===
EXCEL_PATH = "c:\\discord_bot\\active_best_offers_with_sku.xlsx"

# Access them like this:
EBAY_AUTH_TOKEN = os.getenv("EBAY_AUTH_TOKEN")
EBAY_APP_ID = os.getenv("EBAY_APP_ID")
EBAY_DEV_ID = os.getenv("EBAY_DEV_ID")
EBAY_CERT_ID = os.getenv("EBAY_CERT_ID")
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")

# === eBay API connection ===
ebay = Trading(
    appid=EBAY_APP_ID,
    devid=EBAY_DEV_ID,
    certid=EBAY_CERT_ID,
    token=EBAY_AUTH_TOKEN,
    config_file=None
)

def get_ebay_images(item_id, max_images=1, retries=2):
    for attempt in range(retries):
        try:
            response = ebay.execute('GetItem', {
                'ItemID': item_id,
                'DetailLevel': 'ReturnAll'
            })
            pictures = response.dict().get("Item", {}).get("PictureDetails", {}).get("PictureURL", [])
            if isinstance(pictures, list):
                return pictures[:max_images]
        except Exception as e:
            print(f"❌ Attempt {attempt+1} failed for ItemID {item_id}: {e}")
    return []

def format_price(value):
    try:
        return f"${float(value):.2f}"
    except:
        return "N/A"

# === Discord bot setup ===
intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)

class OfferView(discord.ui.View):
    def __init__(self, offer):
        super().__init__(timeout=None)
        self.offer = offer
        self.message = None

    def get_offer_ids(self):
        item_id = str(self.offer.get("ItemID", "")).strip()
        offer_id = str(self.offer.get("BestOfferID", "")).strip()
        return item_id, offer_id

    async def respond_to_offer(self, interaction, action, counter_price=None):
        item_id, offer_id = self.get_offer_ids()

        if not item_id or not offer_id:
            await interaction.response.send_message("❌ Missing offer details — cannot proceed.", ephemeral=True)
            print(f"❌ Missing IDs: ItemID={item_id}, BestOfferID={offer_id}")
            return

        payload = {
            "ItemID": item_id,
            "BestOfferID": offer_id,
            "Action": action
        }

        if action == "Counter":
            payload["CounterOfferPrice"] = counter_price
            payload["CounterOfferQuantity"] = 1

        print("📦 Sending to eBay:", payload)
        await interaction.response.defer(ephemeral=True)

        try:
            response = ebay.execute("RespondToBestOffer", payload)
            print(f"✅ eBay {action} response: {response.dict()}")
            await interaction.followup.send(
                f"✅ {action} offer {offer_id} for Item {item_id}",
                ephemeral=True
            )
            if self.message:
                await self.message.delete()
                print(f"🗑️ Deleted Discord message for BestOfferID {offer_id}")
        except Exception as e:
            error_text = str(e)
            if "Code: 20136" in error_text or "Code: 21929" in error_text or "Code: 20142" in error_text or "Code: 20143" in error_text:
                message = (
                    "⚠️ This best offer is no longer available — it has either expired, "
                    "been withdrawn by the buyer, or you've already responded to it."
                )
            else:
                message = f"❌ Failed to {action.lower()} offer {offer_id}: {error_text}"
            await interaction.followup.send(message, ephemeral=True)
            print(f"❌ eBay {action} error: {error_text}")

    @discord.ui.button(label="✅ Accept Offer", style=discord.ButtonStyle.green)
    async def accept(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.respond_to_offer(interaction, action="Accept")

    @discord.ui.button(label="❌ Decline Offer", style=discord.ButtonStyle.red)
    async def decline(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.respond_to_offer(interaction, action="Decline")

    @discord.ui.button(label="💬 Counter Offer", style=discord.ButtonStyle.blurple)
    async def counter(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(CounterOfferModal(self.offer, self))

class CounterOfferModal(discord.ui.Modal):
    def __init__(self, offer, view):
        super().__init__(title="Submit Counter Offer")
        self.offer = offer
        self.view = view
        self.counter_amount = discord.ui.TextInput(
            label="Counter Offer Amount (USD)",
            placeholder="e.g. 42.50",
            required=True,
            style=discord.TextStyle.short
        )
        self.add_item(self.counter_amount)

    async def on_submit(self, interaction: discord.Interaction):
        amount = self.counter_amount.value.strip()
        try:
            float(amount)
        except ValueError:
            await interaction.response.send_message("❌ Invalid amount format.", ephemeral=True)
            return
        await self.view.respond_to_offer(interaction, action="Counter", counter_price=amount)

@bot.event
async def on_ready():
    await bot.tree.sync()
    print(f"✅ Logged in as {bot.user}")

@bot.tree.command(name="checkoffers", description="Check for new eBay offers for this channel's SKU")
async def checkoffers(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)

    # Normalize channel name: lowercase, no spaces
    channel_name = interaction.channel.name.strip().lower().replace(" ", "")
    print(f"🔎 Channel triggered: '{channel_name}'")

    # Load and normalize Excel
    df = pd.read_excel(EXCEL_PATH, dtype={"ItemID": str, "BestOfferID": str})
    df["ItemID"] = df["ItemID"].fillna("").astype(str).str.strip()
    df["SKU"] = df["SKU"].astype(str).str.strip()
    df["SKU_normalized"] = df["SKU"].str.lower().str.replace(" ", "")
    df["ProcessedToDiscord"] = df["ProcessedToDiscord"].astype(str).str.strip().str.lower()

    # Debug: show what SKUs are available and unprocessed
    print("📦 All SKUs:", df["SKU"].unique())
    print("📦 Unsent SKUs:", df[df["ProcessedToDiscord"].isin(["", "nan"])]["SKU"].unique())

    # Filter for unprocessed offers matching this channel's normalized SKU
    unsent_df = df[
        (df["ProcessedToDiscord"].isin(["", "nan"])) &
        (df["SKU_normalized"] == channel_name)
    ]

    offers = unsent_df.to_dict(orient="records")

    if not offers:
        await interaction.followup.send("📭 No new offers found.", ephemeral=True)
        print(f"📭 No unprocessed offers found for SKU '{channel_name}'")
        return

    await send_offers_to_channel(interaction.channel, offers, df)

    # Clean up and save
    df.drop(columns=["SKU_normalized"], inplace=True)
    try:
        df.to_excel(EXCEL_PATH, index=False)
    except PermissionError:
        print("❌ Excel file is locked — unable to save updates.")
        await interaction.followup.send("⚠️ Offers sent, but Excel file was locked and not updated.", ephemeral=True)
        return

    await interaction.followup.send("✅ Offers sent to this channel.", ephemeral=True)

async def send_offers_to_channel(channel, offers, df):
    for offer in offers:
        item_id = offer.get("ItemID")
        sku = offer.get("SKU", "").strip().lower()

        embed = discord.Embed(title=f"Offer for {sku}", color=discord.Color.blue())
        embed.add_field(name="Ebay Listing Name", value=offer.get("Title"), inline=False)
        embed.add_field(name="Offer Amount", value=format_price(offer.get("OfferAmount")), inline=True)
        embed.add_field(name="Current Listing Price", value=format_price(offer.get("BinPrice")), inline=True)
        embed.add_field(name="Currency", value=offer.get("BinCurrency", "N/A"), inline=True)
        embed.add_field(name="Buyer UserID", value=offer.get("BuyerUserID"), inline=True)
        embed.add_field(name="Offer Expires On", value=offer.get("ExpirationTime (Pacific)"), inline=False)

        buyer_msg = offer.get("BuyerMessage")
        if buyer_msg and str(buyer_msg).strip().lower() != "nan":
            embed.add_field(name="Buyer Message", value=buyer_msg, inline=False)

        image_urls = get_ebay_images(item_id)
        if image_urls:
            embed.set_image(url=image_urls[0])

        view = OfferView(offer)
        msg = await channel.send(embed=embed, view=view)
        view.message = msg

        print(f"✅ Sent offer {offer['BestOfferID']} for SKU '{sku}' in channel '{channel.name}'")

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df.loc[df["BestOfferID"] == offer["BestOfferID"], "ProcessedToDiscord"] = "Y"
        df.loc[df["BestOfferID"] == offer["BestOfferID"], "DateTimeProcessedToDiscord"] = now

bot.run(TOKEN)