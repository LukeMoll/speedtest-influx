import platform
import argparse, configparser
import csv
from pathlib import Path
import os
import influxdb
import speedtest

def main():
    config_fn = "config.ini"

    args = do_args()
    config = get_config(config_fn)
    if args.import_csv is None:
        data = [do_test()]
    else:
        with open(args.import_csv) as fd:
            data = list(import_csv(fd, has_headers=args.skip))
    override_dict = {
        "tags": {
            "hostname": args.hostname if args.hostname else None
        }
    }
    override_data(data, override_dict)
    try:
        influx_write(
            data,
            config['influxdb']['host'],
            config['influxdb'].get('port', 8086),
            config['influxdb']['username'],
            config['influxdb']['password'],
            config['influxdb']['database'],
            config['influxdb']['measurement']
        )
    except KeyError as e:
        print("Missing {} section/value in {}".format(e, config_fn))


def do_args():
    parser = argparse.ArgumentParser(description='Perform a SpeedTest.net test and write the result to InfluxDB')
    parser.add_argument('--import', metavar="FILENAME", help="Imports data from a speedtest-cli CSV file", dest="import_csv")
    parser.add_argument('--config', metavar="FILENAME", help="Path to an alternative config file. (default: config.ini)", default="config.ini", dest="config_fn")
    parser.add_argument('--skip', action="store_true", help="If used with --import, skips the first line (header) of CSV file.")
    parser.add_argument('--hostname', help="Alternative hostname to store data under (defaults to system hostname)")
    return parser.parse_args()


def getLength():
    with open('main.py') as fd:
        return sum(len(line) for line in fd.readlines())

def get_config(fn):
    if not os.path.exists(fn):
        raise FileNotFoundError("Could not find {}".format(fn))
    config = configparser.ConfigParser()
    config.read(fn)

    return config

def override_data(data, override_dict):
    # I was very tired when I wrote this
    for datum in data:
        for k in datum.keys():
            if k in override_dict and type(override_dict[k] is dict):
                for l,u in override_dict[k].items():
                    if u is not None: datum[k][l] = u

headers = {
    "Server_ID": 0,
    "Sponsor": 1,
    "Server_Name": 2,
    "Timestamp": 3,
    "Distance": 4,
    "Ping": 5,
    "Download": 6,
    "Upload": 7,
    "Share": 8,
    "IP_Address": 9
}

def do_test():
    s = speedtest.Speedtest()
    s.get_servers([])
    s.get_best_server()
    s.download()
    s.upload()
    results = s.results.dict()
    return {
        "tags": {
            "hostname": platform.node(),
            "server":   results['server']['id'],
            "ip":       results['client']['ip']
        },
        "fields": {
            "upload_bps":   results['upload'],
            "download_bps": results['download'],
            "ping_ms": 	    results['ping'],
            "tx_bytes":     results['bytes_sent'],
            "rx_bytes":     results['bytes_received'],
            "length":       getLength()
        },
        "time": results['timestamp']
    }

def influx_write(data, host, port, username, password, database, measurement):
    for datum in data: datum['measurement'] = measurement
    client = influxdb.InfluxDBClient(host, port, username, password, database, ssl=True)
    client.write_points(data)

def import_csv(fd, hostname=platform.node(), has_headers=False):
    reader = csv.reader(fd)
    if has_headers: next(reader, None)

    for row in reader:
        yield {
            "tags": {
                "hostname": hostname,
                "server": str(row[headers['Server_ID']]),
                "ip": row[headers['IP_Address']]
            },
            "fields": {
                "upload_bps":  float(row[headers['Upload']]),
                "download_bps": float(row[headers['Download']]),
                "ping_ms": float(row[headers['Ping']]),
            },
            "time": row[headers['Timestamp']]
        }

if __name__ == "__main__":
    main()