#!/usr/bin/env python3
"""
Modular Trading Bot - Supports multiple exchanges
"""

import argparse
import asyncio
import logging
from pathlib import Path
import sys
import dotenv
from decimal import Decimal
from trading_bot import TradingBot, TradingConfig
from exchanges import ExchangeFactory


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Modular Trading Bot - Supports multiple exchanges')

    # Exchange selection
    parser.add_argument('--exchange', type=str, default='edgex',
                        choices=ExchangeFactory.get_supported_exchanges(),
                        help='Exchange to use (default: edgex). '
                             f'Available: {", ".join(ExchangeFactory.get_supported_exchanges())}')

    # Trading parameters
    parser.add_argument('--ticker', type=str, default='ETH',
                        help='Ticker (default: ETH)')
    parser.add_argument('--quantity', type=Decimal, default=Decimal(0.1),
                        help='Order quantity (default: 0.1)')
    parser.add_argument('--take-profit', type=Decimal, default=Decimal(0.02),
                        help='Take profit in USDT (default: 0.02)')
    parser.add_argument('--direction', type=str, default='buy', choices=['buy', 'sell'],
                        help='Direction of the bot (default: buy)')
    parser.add_argument('--max-orders', type=int, default=40,
                        help='Maximum number of active orders (default: 40)')
    parser.add_argument('--wait-time', type=int, default=450,
                        help='Wait time between orders in seconds (default: 450)')
    parser.add_argument('--env-file', type=str, default=".env",
                        help=".env file path (default: .env)")
    parser.add_argument('--grid-step', type=str, default='-100',
                        help='The minimum distance in percentage to the next close order price (default: -100)')
    parser.add_argument('--stop-price', type=Decimal, default=-1,
                        help='Price to stop trading and exit. Buy: exits if price >= stop-price.'
                        'Sell: exits if price <= stop-price. (default: -1, no stop)')
    parser.add_argument('--pause-price', type=Decimal, default=-1,
                        help='Pause trading and wait. Buy: pause if price >= pause-price.'
                        'Sell: pause if price <= pause-price. (default: -1, no pause)')
    parser.add_argument('--boost', action='store_true',
                        help='Use the Boost mode for volume boosting')
    
    # Profit protection parameters
    parser.add_argument('--pp', nargs='*', type=float, default=None,
                        help='Profit protection: --pp [xx] [yy]. '
                             'When profit exceeds xx%%, start profit protection. '
                             'When profit protection is active, if profit drawdown >= yy%% from peak, close all positions and enter silent mode. '
                             'Default: xx=5, yy=50 if no args; yy=50 if only xx provided')
    
    # Stop loss parameters
    parser.add_argument('--sl', nargs='?', type=float, default=None, const=10.0,
                        help='Stop loss: --sl [xx]. '
                             'If account drops by xx%% from initial capital, enter silent mode. '
                             'Default: xx=10%% if no value provided')
    
    # Silent mode parameters
    parser.add_argument('--sm', nargs='?', type=int, default=None, const=60,
                        help='Silent mode: --sm [xx]. '
                             'After profit protection or stop loss triggers, pause for xx minutes, then reset all parameters and restart. '
                             'Default: xx=60 if no value provided. '
                             'If --pp or --sl is set without --sm, --sm defaults to 60')

    return parser.parse_args()


def setup_logging(log_level: str):
    """Setup global logging configuration."""
    # Convert string level to logging constant
    level = getattr(logging, log_level.upper(), logging.INFO)

    # Clear any existing handlers to prevent duplicates
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Configure root logger WITHOUT adding a console handler
    # This prevents duplicate logs when TradingLogger adds its own console handler
    root_logger.setLevel(level)

    # Suppress websockets debug logs unless DEBUG level is explicitly requested
    if log_level.upper() != 'DEBUG':
        logging.getLogger('websockets').setLevel(logging.WARNING)

    # Suppress other noisy loggers
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)

    # Suppress Lighter SDK debug logs
    logging.getLogger('lighter').setLevel(logging.WARNING)
    # Also suppress any root logger DEBUG messages that might be coming from Lighter
    if log_level.upper() != 'DEBUG':
        # Set root logger to WARNING to suppress DEBUG messages from Lighter SDK
        root_logger.setLevel(logging.WARNING)


async def main():
    """Main entry point."""
    args = parse_arguments()

    # Setup logging first
    setup_logging("WARNING")

    # Validate boost-mode can only be used with aster and backpack exchange
    if args.boost and args.exchange.lower() != 'aster' and args.exchange.lower() != 'backpack':
        print(f"Error: --boost can only be used when --exchange is 'aster' or 'backpack'. "
              f"Current exchange: {args.exchange}")
        sys.exit(1)

    env_path = Path(args.env_file)
    if not env_path.exists():
        print(f"Env file not find: {env_path.resolve()}")
        sys.exit(1)
    dotenv.load_dotenv(args.env_file)

    # Parse and validate profit protection parameters
    profit_protection_threshold = None
    profit_protection_drawdown = None
    if args.pp is not None:
        if len(args.pp) == 0:
            # No arguments: default xx=5, yy=50
            profit_protection_threshold = Decimal(5)
            profit_protection_drawdown = Decimal(50)
        elif len(args.pp) == 1:
            # One argument: xx provided, default yy=50
            profit_protection_threshold = Decimal(args.pp[0])
            profit_protection_drawdown = Decimal(50)
        elif len(args.pp) == 2:
            # Two arguments: both xx and yy provided
            profit_protection_threshold = Decimal(args.pp[0])
            profit_protection_drawdown = Decimal(args.pp[1])
        else:
            print("Error: --pp accepts at most 2 arguments (xx and yy)")
            sys.exit(1)
        
        # Validate parameters
        if profit_protection_threshold <= 0 or profit_protection_drawdown <= 0:
            print(f"Error: --pp parameters must be > 0. Got xx={profit_protection_threshold}, yy={profit_protection_drawdown}")
            print("请重新设置参数 (Please reset parameters)")
            sys.exit(1)
    
    # Parse and validate stop loss parameters
    stop_loss_threshold = None
    if args.sl is not None:
        stop_loss_threshold = Decimal(args.sl)
        if stop_loss_threshold <= 0:
            print(f"Error: --sl parameter must be > 0. Got xx={stop_loss_threshold}")
            print("请重新设置参数 (Please reset parameters)")
            sys.exit(1)
    
    # Parse and validate silent mode parameters
    silent_mode_duration = None
    if args.sm is not None:
        silent_mode_duration = args.sm
        if silent_mode_duration <= 0:
            print(f"Error: --sm parameter must be > 0. Got xx={silent_mode_duration}")
            print("请重新设置参数 (Please reset parameters)")
            sys.exit(1)
    elif profit_protection_threshold is not None or stop_loss_threshold is not None:
        # If pp or sl is set but sm is not, default sm to 60
        silent_mode_duration = 60

    # Create configuration
    config = TradingConfig(
        ticker=args.ticker.upper(),
        contract_id='',  # will be set in the bot's run method
        tick_size=Decimal(0),
        quantity=args.quantity,
        take_profit=args.take_profit,
        direction=args.direction.lower(),
        max_orders=args.max_orders,
        wait_time=args.wait_time,
        exchange=args.exchange.lower(),
        grid_step=Decimal(args.grid_step),
        stop_price=Decimal(args.stop_price),
        pause_price=Decimal(args.pause_price),
        boost_mode=args.boost,
        profit_protection_threshold=profit_protection_threshold,
        profit_protection_drawdown=profit_protection_drawdown,
        stop_loss_threshold=stop_loss_threshold,
        silent_mode_duration=silent_mode_duration
    )

    # Create and run the bot
    bot = TradingBot(config)
    try:
        await bot.run()
    except Exception as e:
        print(f"Bot execution failed: {e}")
        # The bot's run method already handles graceful shutdown
        return


if __name__ == "__main__":
    asyncio.run(main())
