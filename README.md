# fleaBay

Conda package for Python that connects to the eBay API and obtains information regarding completed auctions for a given product category. After information is collected via the API access, auction listings are scrapped for additional attribute information (e.g., product description). Also includes an ETL process to  monitor the source database and routinely extract, transform, and load data from the source database into a data warehouse.

## Install

Install the latest version of fleaBay using Conda

```bash
$ conda install fleabay
```

## Usage
To use the `fleaBay` command line interface:
```bash
$ fleabay --help
```

To run `fleaBay` on a regular schedule using `cron`:

```bash
00 12 * * * /home/curtis/Program_Files/miniconda2/envs/fleabay-dev/bin/python /home/curtis/github/fleaBay/fleabay/start_fleabay.py > /home/curtis/github/fleaBay/fleabay.log 2>&1
```


