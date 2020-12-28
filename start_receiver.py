import argparse
import logging

import yaml
from workers.data_receiver import BinanceWebSocketReceiver


def main(config):
    streams = config['streams']
    symbols = config['symbols'] if config['symbols'] else None

    if symbols:
        symbols = list(map(lambda x: x.lower(), symbols))
    if streams is not None:
        streams = list(map(lambda x: x.lower(), streams))

    manager = BinanceWebSocketReceiver(symbols, streams)
    manager.start_websocket()


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
                        filename='logs/binance_ws_receiver.log',
                        format='%(asctime)s - %(levelname)s: %(message)s')
    main(loaded_config)
