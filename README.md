# fleaBay

.. image:: https://img.shields.io/travis/CurtLH/fleabay.svg
        :target: https://travis-ci.org/CurtLH/fleabay


fleaBay is a Python package for collecting data from the eBay API.  

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


