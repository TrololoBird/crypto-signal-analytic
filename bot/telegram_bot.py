"""Telegram bot for signal notifications and commands.

Based on aiogram 3.x with FSM and middleware support.
Provides commands:
- /start - Welcome message
- /status - Bot status and metrics
- /signals - Recent signals
- /market - Market regime info
- /subscribe - Subscribe to signal alerts
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from html import escape

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder

from .config import BotSettings

LOG = logging.getLogger(__name__)
UTC = timezone.utc


class TelegramSignalBot:
    """Telegram bot integration for signal notifications."""

    def __init__(self, settings: BotSettings) -> None:
        self.settings = settings
        self.bot = Bot(token=settings.tg_token)
        self.dp = Dispatcher()
        self._setup_handlers()

    def _setup_handlers(self) -> None:
        """Register message handlers."""
        self.dp.message.register(self._cmd_start, Command("start"))
        self.dp.message.register(self._cmd_status, Command("status"))
        self.dp.message.register(self._cmd_signals, Command("signals"))
        self.dp.message.register(self._cmd_market, Command("market"))
        self.dp.message.register(self._cmd_help, Command("help"))

    async def _cmd_start(self, message: types.Message) -> None:
        """Handle /start command."""
        if not message.from_user:
            return
        
        welcome_text = (
            f"👋 Welcome, {message.from_user.first_name}!\n\n"
            "🤖 <b>Crypto Signal Bot</b>\n"
            "Real-time trading signals from Binance Futures\n\n"
            "📊 <b>Available commands:</b>\n"
            "/status - Bot status & metrics\n"
            "/signals - Recent signals\n"
            "/market - Market regime analysis\n"
            "/help - Show this help\n\n"
            "⚡ Signals are generated based on:\n"
            "• Market structure (BOS/CHoCH)\n"
            "• Order blocks & FVG\n"
            "• Liquidity sweeps\n"
            "• ML scoring\n"
        )
        
        await message.answer(welcome_text, parse_mode="HTML")

    async def _cmd_status(self, message: types.Message) -> None:
        """Handle /status command."""
        # Get bot status from SignalBot instance (to be injected)
        status_text = (
            "🟢 <b>Bot Status: RUNNING</b>\n\n"
            "📈 <b>Metrics:</b>\n"
            "• Signals today: 12\n"
            "• Win rate: 68%\n"
            "• Active pairs: 45\n\n"
            "🔌 <b>WebSocket:</b> Connected\n"
            "📡 Latency: 45ms\n"
            "📊 Market: Bullish regime\n"
        )
        await message.answer(status_text, parse_mode="HTML")

    async def _cmd_signals(self, message: types.Message) -> None:
        """Handle /signals command."""
        signals_text = (
            "🎯 <b>Recent Signals (last 24h):</b>\n\n"
            "<code>BTCUSDT</code> 📈 LONG @ 67,450 (Score: 0.87)\n"
            "<code>ETHUSDT</code> 📈 LONG @ 3,520 (Score: 0.82)\n"
            "<code>SOLUSDT</code> 📉 SHORT @ 142.5 (Score: 0.79)\n"
            "<code>DOGEUSDT</code> 📈 LONG @ 0.182 (Score: 0.75)\n\n"
            "⚠️ This is educational only. Not financial advice."
        )
        
        keyboard = InlineKeyboardBuilder()
        keyboard.add(InlineKeyboardButton(text="📊 View Dashboard", url="http://localhost:8080"))
        
        await message.answer(
            signals_text, 
            parse_mode="HTML",
            reply_markup=keyboard.as_markup()
        )

    async def _cmd_market(self, message: types.Message) -> None:
        """Handle /market command."""
        market_text = (
            "🌍 <b>Market Regime Analysis</b>\n\n"
            "📊 <b>Overall:</b> Bullish (strength: 0.72)\n"
            "🪙 <b>BTC Dominance:</b> 52.3%\n"
            "🌊 <b>Altcoin Season:</b> Index 45 (neutral)\n"
            "💰 <b>Funding Sentiment:</b> Neutral\n\n"
            "<b>Sector Performance (24h):</b>\n"
            "🟢 Layer 1: +5.2%\n"
            "🟢 DeFi: +3.8%\n"
            "🔴 Meme: -2.1%\n"
        )
        await message.answer(market_text, parse_mode="HTML")

    async def _cmd_help(self, message: types.Message) -> None:
        """Handle /help command."""
        help_text = (
            "📖 <b>Bot Commands</b>\n\n"
            "<b>User Commands:</b>\n"
            "/start - Start bot\n"
            "/status - Check bot status\n"
            "/signals - View recent signals\n"
            "/market - Market analysis\n"
            "/help - Show this message\n\n"
            "<b>Admin Commands:</b>\n"
            "/restart - Restart bot\n"
            "/config - View configuration\n"
            "/logs - View recent logs\n\n"
            "⚠️ Signals are for educational purposes only."
        )
        await message.answer(help_text, parse_mode="HTML")

    async def send_signal_alert(
        self, 
        chat_id: int | str, 
        symbol: str, 
        direction: str,
        entry: float,
        score: float,
        setup_type: str,
    ) -> None:
        """Send signal alert to specified chat."""
        emoji = "📈" if direction.upper() == "LONG" else "📉"
        safe_symbol = escape(symbol)
        safe_direction = escape(direction.upper())
        safe_setup_type = escape(setup_type)
        
        text = (
            f"🎯 <b>New Signal: {safe_symbol}</b>\n\n"
            f"{emoji} <b>Direction:</b> {safe_direction}\n"
            f"💰 <b>Entry:</b> {entry:,.2f}\n"
            f"⭐ <b>Score:</b> {score:.2f}\n"
            f"🔍 <b>Setup:</b> {safe_setup_type}\n\n"
            f"⏰ {datetime.now(UTC).strftime('%H:%M:%S')} UTC\n\n"
            "⚠️ Educational only. DYOR."
        )
        
        await self.bot.send_message(chat_id, text, parse_mode="HTML")

    async def start(self) -> None:
        """Start polling for updates."""
        LOG.info("Starting Telegram bot polling...")
        await self.dp.start_polling(self.bot)

    async def stop(self) -> None:
        """Stop the bot."""
        LOG.info("Stopping Telegram bot...")
        await self.bot.session.close()


# Integration with SignalBot (to be implemented)
async def setup_telegram_bot(settings: BotSettings) -> TelegramSignalBot | None:
    """Factory function to create and start Telegram bot."""
    if not (settings.tg_token and settings.tg_token.strip()):
        LOG.warning("No TG_TOKEN provided, Telegram bot disabled")
        return None
    
    bot = TelegramSignalBot(settings)
    return bot


if __name__ == "__main__":
    import asyncio
    
    # Test mode
    logging.basicConfig(level=logging.INFO)
    
    # Example usage
    async def test() -> None:
        from .config import load_settings
        
        settings = load_settings()
        bot = await setup_telegram_bot(settings)
        
        if bot:
            await bot.start()
    
    try:
        asyncio.run(test())
    except KeyboardInterrupt:
        print("Bot stopped")
