import argparse
import logging

import yaml
from dotenv import load_dotenv

from workers.data_checker import BinanceDataChecker

load_dotenv()


def main(config):
    sleep_time = config['data_checker']['sleep_time']

    manager = BinanceDataChecker(sleep_time)
    manager.start_checking()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help='Provide config path', type=str)
    parser.add_argument('--debug', type=bool)
    args = parser.parse_args()

    config_path, debug = args.config, args.debug
    with open(config_path, 'r') as file:
        loaded_config = yaml.safe_load(file)

    logging_level = logging.DEBUG if args.debug is not None else logging.INFO
    logging.basicConfig(level=logging_level,
                        filename='logs/binance_ws_checker.log',
                        format='%(asctime)s - %(levelname)s: %(message)s')
    main(loaded_config)
