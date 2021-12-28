# Divar.ir Ads Scrapper
# Introduction
This project first asynchronously grab Divar.ir Ads and then save to `.csv`
and `.xlsx` files named `data.csv` and `data.xlsx` which contains 'Almost' all
features that an Ad has on Divar.ir website.

# Usage

```commandline
python3.9 -m venv venv
source ./venv/bin/activate
pip install -r requirements.txt
python main.py
```

# Parameters

There are some parameters defined at the first of `main.py` which can modify
some behaviours of program.

With higher `WORKERS` value you get more async workers, but it rises the risk of
getting `HTTP 429 Too Many Requests` response status.

If you want to scrap all pages of the website, put `THRESHOLD` to an high value.

By default, this script scrap all Ads of apartments in `Niavaran` and `District 1`.
You can change `AD_LIST_URL` in order to get another desired Ads.

# Parameters

There are some parameters defined at the first of `main.py` which can modify
some behaviours of program.

With higher `WORKERS` value you get more async workers, but it rises the risk of
getting `HTTP 429 Too Many Requests` response status.

If you want to scrap all pages of the website, put `THRESHOLD` to an high value.

By default, this script scrap all Ads of apartments in `Niavaran` and `District 1`.
You can change `AD_LIST_URL` in order to get another desired Ads.
