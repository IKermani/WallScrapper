import sys
import re
import time
import itertools
import threading
import asyncio

from datetime import timedelta
from typing import TypeVar
from asyncio import Queue, BoundedSemaphore, create_task, as_completed, gather

import jdatetime
import pandas as pd
from tqdm import tqdm
from requests_html import AsyncHTMLSession

# The number of workers to scrap the API asynchronously
WORKERS = 16
# The Threshold for number of pages to scrap
THRESHOLD = 10000000
# Total number of pages of the API
TOTAL_PAGES = None

# Niavaran and District 1 Ads list base URL.
# different pages can be access via appending desired page number to the end of this string.
AD_LIST_URL = 'https://api.divar.ir/v8/web-search/tehran/buy-apartment/'
# Ad detail URL; The base URL which Ad details can be reached.
# By appending Ad ID (in this API Ad ID is distinguished by keyword "token") to the end of
# this string the Ad details shall be reached.
AD_DETAIL_URL = 'https://api.divar.ir/v5/posts/'
# Ad contact number base URL; This website has implementing GET Throttling contact number
AD_CONTACT_DETAIL_URL = 'https://api.divar.ir/v5/posts/{}/contact'

# Already authenticated cookies in order to extract the Ad-Owner phone number.
COOKIES = {
    'did': '8a0c7c3e-9fed-4ece-8bb9-9e50b3418d0c',
    '_gcl_au': '1.1.606995357.1640380110',
    'token': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyIjoiMDkxOTQ2OTg2NzYiLCJpc3MiOiJhdXRoIiwiaWF0IjoxNjQwNjc2MjA3LCJleHAiOjE2NDE5NzIyMDcsInZlcmlmaWVkX3RpbWUiOjE2NDA2NzYyMDUsInVzZXItdHlwZSI6InBlcnNvbmFsIiwidXNlci10eXBlLWZhIjoiXHUwNjdlXHUwNjQ2XHUwNjQ0IFx1MDYzNFx1MDYyZVx1MDYzNVx1MDZjYyIsInNpZCI6IjgxNWQxMDVkLTY2NjEtNDM0Yi04ZGVjLTE0ZDM0OTJkYmY2ZSJ9.scxUtin3wVdbQIyVoXeaiodnitHXxiB85uyKaD7TU_s',
}

# All columns of the DataSet
COLUMNS = [
    'ad_id', 'ad_title', 'contact_number', 'ad_owner_id', 'ad_owner_name',
    'ad_owner_type', 'ad_owner_phonenumber', 'ad_owner_address', 'price', 'square_footage',
    'construction_year', 'number_of_rooms', 'price', 'price_per_square_footage', 'floor',
    'floor', 'elevator', 'parking', 'storage', 'number_of_units_per_floor', 'writ',
    'building_direction', 'unit_status', 'balcony', 'western_toilet', 'underfloor_heating',
    'hot_water_supplier', 'floor_material', 'address', 'coordinates', 'description',
    'ad_owner_short_address',
]

# The datetime format to store the datetime values in the dataset
DATETIME_FORMAT = '%Y/%m/%d %H:%M:%S'

# values of datetime in divar.ir
JUST_NOW = 'لحظاتی پیش'
FEW_MINUTES_BEFORE = 'دقایقی پیش'
QUARTER = 'ربع'
HALF = 'نیم'
HOUR = 'ساعت'
YESTERDAY = 'دیروز'
TWO_DAYS_AGO = 'پریروز'
N_DAYS_AGO = 'روز پیش'
WEEK = 'هفتهٔ'

# The list represents the whole dataset and each dict in it represents a row.
list_of_dicts = []

# values to perform loading animation :)
MESSAGE = ''
DONE = False

ENTRYPOINT_URL = None
NEIGHBORHOOD = None
QUERY_PARAMS = '?'


def load_config(config_filename='config.xlsx'):
    global NEIGHBORHOOD, ENTRYPOINT_URL, QUERY_PARAMS, AD_LIST_URL

    _df = pd.read_excel(config_filename, na_filter=False)
    NEIGHBORHOOD = _df[_df['key'] == 'neighborhood'].value.values[0]
    ENTRYPOINT_URL = _df[_df['key'] == 'entrypoint_url'].value.values[0]

    params = _df[(_df['key'] != 'neighborhood') & (_df['key'] != 'entrypoint_url')].values
    for param in params:
        key, value = param
        QUERY_PARAMS += str(f'{key}={value}&')

    if not ENTRYPOINT_URL and not NEIGHBORHOOD:
        print(f"please set entrypoint_url or neighborhood values in config.xlsx")
        exit(1)

    if ENTRYPOINT_URL:
        ENTRYPOINT_URL = str(ENTRYPOINT_URL)
        if len(splited := ENTRYPOINT_URL.rsplit('?')) > 1:
            # got some filter
            ENTRYPOINT_URL += '&page='
        else:
            # no filter is used
            ENTRYPOINT_URL += '?page='
        AD_LIST_URL = ENTRYPOINT_URL
        NEIGHBORHOOD = ENTRYPOINT_URL.rsplit('?')[0].rsplit('/')[-1]
    else:
        AD_LIST_URL += str(NEIGHBORHOOD)
        AD_LIST_URL += QUERY_PARAMS + 'page='


# elegant way to show the logs of this script
def print_(msg):
    msg = str(msg)
    print(f'\r\n[i] {msg}')


# animation for loading.
def animate():
    for c in itertools.cycle(["⢿", "⣻", "⣽", "⣾", "⣷", "⣯", "⣟", "⡿"]):
        if DONE:
            break
        sys.stdout.write(f'\r[i] {MESSAGE} {c}')
        sys.stdout.flush()
        time.sleep(0.2)


def text2timestamp(text):
    """
    Gets string time from website and convert it to time delta
    and then returns the subtracted timedelta from now in jalali
    format
    """
    time_delta = {'minutes': 0}

    if JUST_NOW in text:
        pass
    elif FEW_MINUTES_BEFORE:
        time_delta = {'minutes': 5}
    elif QUARTER in text:
        time_delta = {'minutes': 15}
    elif HALF in text:
        time_delta = {'minutes': 30}
    elif HOUR in text:
        hours = int(re.sub(r"\D", "", text))
        time_delta = {'hours': hours}
    elif YESTERDAY in text:
        time_delta = {'days': 1}
    elif TWO_DAYS_AGO in text:
        time_delta = {'days': 2}
    elif N_DAYS_AGO in text:
        n = int(re.sub(r"\D", "", text))
        time_delta = {'days': n}
    elif WEEK in text:
        n = int(re.sub(r"\D", "", text) or 1)
        time_delta = {'weeks': n}
    else:
        return text

    return (jdatetime.datetime.now() - timedelta(**time_delta)).strftime(DATETIME_FORMAT)


async def ad_list_worker(job_q: Queue, res_q: Queue, asession: AsyncHTMLSession = AsyncHTMLSession()):
    """
    This function is a async worker which waits for a new
    job and at new job arrival get the related page and put
    the response (which contains Ads of job=page_number) to
    the Queue to ultimately get the page details.
    :param job_q:
    :param res_q:
    :param asession:
    :return:
    """
    while True:
        page = await job_q.get()
        res = await asession.get(AD_LIST_URL + str(page))
        await res_q.put((res, page))
        job_q.task_done()


async def ad_detail_worker(job_q: Queue, res_q: Queue, pbar, asession: AsyncHTMLSession = AsyncHTMLSession()):
    """
    Waits for a new job of Ad detail scraping become present in the
    Queue and at the presence of new job GET it and put the response
    in the response queue to be processed later.
    """
    while True:
        ad_id = await job_q.get()
        res = await asession.get(AD_DETAIL_URL + str(ad_id))
        await res_q.put((res, ad_id))
        job_q.task_done()
        pbar.update(1)


async def get_ads_detail(queues, asession):
    """
    Create ad_detail_workers and manage how they should work.
    """
    # if the contact throttle is reached this flag will be true and
    # script will no longer GET contact phone numbers.
    CONTACT_THROTTLE_THRESHOLD_REACHED = False

    list_queues, detail_queues = queues
    ad_detail_jobs_q, ad_detail_res_q = detail_queues

    total_jobs = ad_detail_jobs_q.qsize()

    # create progress bar to track the process of the script
    pbar = tqdm(total=total_jobs, desc='Fetching Ads Details')

    tasks = []
    # create WORKER number of tasks to handle the fetching ad details
    for _ in range(1, WORKERS + 1):
        task = create_task(ad_detail_worker(ad_detail_jobs_q, ad_detail_res_q, pbar, asession))
        tasks.append(task)

    # tracks the number of jobs done
    jobs_done = 0

    while True:
        # new data record
        data = {k: None for k in COLUMNS}

        # wait for new job become available
        res, ad_id = await ad_detail_res_q.get()
        res = res.json()

        # if Throttling Threshold was not reached
        if not CONTACT_THROTTLE_THRESHOLD_REACHED:
            # get the ad_owner contact phone number
            contact_res = await asession.get(AD_CONTACT_DETAIL_URL.format(ad_id))
            contact_res = contact_res.json()
            if not contact_res.get('error'):
                data['contact_number'] = contact_res['widgets']['contact']['phone']
            else:
                # raise flag
                CONTACT_THROTTLE_THRESHOLD_REACHED = True
                sys.stdout.flush()
                print_('Contact Throttle threshold reached.')
                data['contact_number'] = None

        # region extract_info + data cleaning
        try:
            # Ad ID
            data['ad_id'] = res['token']
        except KeyError:
            # this will happen when the worker number and the network speed is high
            # WORKER = 16 will not raise this error
            print_('\n!!!ERROR HTTP429: too many requests!!!\nTry lowering WORKERS number!!!')
            exit(1)
        data['ad_title'] = res['data']['share']['title']
        data['ad_publish_datetime'] = text2timestamp(res['widgets']['header']['date'])
        data['ad_scrap_timestamp'] = jdatetime.datetime.now().strftime(DATETIME_FORMAT)
        data['district'] = res['data']['district']

        ad_owner_type = res['data']['business_data']['business_type']
        data['ad_owner_type'] = ad_owner_type
        if ad_owner_type == 'personal':
            data['ad_owner_id'] = None
            data['ad_owner_name'] = None
            data['ad_owner_phonenumber'] = None
            data['ad_owner_address'] = None
        else:
            data['ad_owner_id'] = res['data']['business_data']['data']['id']
            data['ad_owner_name'] = res['data']['business_data']['data']['name']
            data['ad_owner_phonenumber'] = res['data']['business_data']['data'].get('telephoneNumber')
            data['ad_owner_address'] = res['data']['business_data']['data'].get('address')
            data['ad_owner_short_address'] = res['data']['business_data']['data'].get('shortAddress')

        data['price'] = res['data']['webengage']['price']

        for d in res['widgets']['list_data']:
            title = d.get('title')
            value = d.get('value')
            items = d.get('items')

            if title == 'اطلاعات':
                for i in items:
                    key = i.get('title')
                    val = i.get('value')
                    if key == 'متراژ':
                        data['square_footage'] = int(val)
                    elif key == 'ساخت':
                        data['construction_year'] = int(re.sub(r"\D", "", val))
                    elif key == 'اتاق':
                        if 'بدون اتاق' in val:
                            val = 0
                        data['number_of_rooms'] = int(val)
                    else:
                        data[key] = val

            elif title == 'آگهی‌دهنده':
                data['ad_owner_name'] = value

            elif title == 'قیمت هر متر':
                # convert Tomas value to Rials
                if 'تومان' in value:
                    value = int(value.strip('تومان').strip().replace('٬', ''))
                data['price_per_square_footage'] = value

            elif title == 'طبقه':
                floor = value
                if len(floor.split()) > 1:
                    floor = floor.split()[0]
                elif 'از' in floor:
                    floor = floor.split('از')[0]

                if floor == 'همکف':
                    floor = 0
                if floor == 'زیرهمکف':
                    floor = -1

                data['floor'] = int(floor)

            elif title == 'ویژگی‌ها و امکانات':
                for item in items:
                    t = item.get('title')
                    val = item.get('available')

                    if 'آسانسور' in t:
                        data['elevator'] = val

                    elif 'پارکینگ' in t:
                        data['parking'] = val

                    elif 'انباری' in t:
                        data['storage'] = val

                extra_features = d.get('next_page')

                if extra_features:
                    for feature in extra_features.get('widget_list'):
                        feature_title = feature['data'].get('title')
                        feature_value = feature['data'].get('value')

                        if feature_title:
                            if feature_title == 'تعداد واحد در طبقه':
                                data['number_of_units_per_floor'] = int(re.sub(r"\D", "", feature_value))

                            elif feature_title == 'سند':
                                data['writ'] = feature_value

                            elif feature_title == 'جهت ساختمان':
                                data['building_direction'] = feature_value

                            elif feature_title == 'وضعیت واحد':
                                data['unit_status'] = feature_value

                            elif 'آسانسور' in feature_title:
                                if data['elevator'] == None:
                                    data['elevator'] = feature_value

                            elif 'پارکینگ' in feature_title:
                                if data['parking'] == None:
                                    data['parking'] = feature_value

                            elif 'انباری' in feature_title:
                                if data['storage'] == None:
                                    data['storage'] = feature_value

                            elif 'بالکن' in feature_title:
                                data['balcony'] = feature_value

                            elif 'فرنگی' in feature_title or 'ایرانی' in feature_title:
                                if 'ایرانی' in feature_title:
                                    data['iranian_toilet'] = True

                                if 'فرنگی' in feature_title:
                                    data['western_toilet'] = True

                            elif 'سرمایش' in feature_title:
                                data['cooling_system'] = feature_title.replace('سرمایش', '').strip()

                            elif 'گرمایش' in feature_title:
                                if feature_title == 'گرمایش از کف':
                                    data['underfloor_heating'] = True
                                else:
                                    data['cooling_system'] = feature_title.replace('گرمایش', '').strip()

                            elif 'تأمین‌کننده آب گرم' in feature_title:
                                data['hot_water_supplier'] = feature_title.replace('تأمین‌کننده آب گرم', '').strip()

                            elif 'جنس کف' in feature_title:
                                data['floor_material'] = feature_title.split('جنس کف ')[-1]

                            else:
                                data[feature_title] = feature_value if feature_value else feature_title

        # Divar.ir does not have address for the apartment
        data['address'] = None

        data['coordinates'] = f'{res["widgets"]["location"]["latitude"]},{res["widgets"]["location"]["longitude"]}' if \
            res["widgets"]["location"] else None

        data['images'] = str(res['widgets']['images'])

        data['description'] = res['data']['description']

        list_of_dicts.append(data)

        # endregion

        # a job is done so raise jobs done !
        jobs_done += 1

        if jobs_done == total_jobs:
            break

    await ad_detail_jobs_q.join()

    # cancel our workers
    for task in tasks:
        task.cancel()
    # gather all responses (which is none here and we don't want it).
    await gather(*tasks, return_exceptions=True)


async def get_ads_list(queues, asession: AsyncHTMLSession = AsyncHTMLSession()):
    """
    handles the process of creating ad list workers and
    handles where is the last page of Ads (to stop the process = while loop).
    """

    def _is_last_page(res):
        """
        check if the response is last page or not
        """
        if res.get('last_post_date') == -1:
            return True
        return False

    def _get_ads_ids(res):
        """
        extract AD IDs (or tokens) from the page
        """
        return [ad.get('data').get('token') for ad in res.get('widget_list')]

    global MESSAGE
    global DONE

    # mutext for updating values
    LOCK = asyncio.Lock()
    CREATE_TASK = True

    _last_scraped_page = 1
    _last_page_to_be_scraped = 1
    ad_list_tasks = []

    # getting Queues
    list_queues, detail_queues = queues
    ad_list_jobs_q, ad_list_res_q = list_queues
    ad_detail_jobs_q, ad_detail_res_q = detail_queues

    print_('creating jobs...')
    for _ in range(1, WORKERS + 1):
        task = create_task(ad_list_worker(ad_list_jobs_q, ad_list_res_q, asession))
        ad_list_tasks.append(task)
    print_('jobs created.')

    # start the process of getting pages with a window of width size(WORKER)
    scrape_range = (1, _last_scraped_page + 1 * WORKERS + 1)
    _last_page_to_be_scraped = scrape_range[1] - 1

    print_(f'Starting scraper width {WORKERS}.')
    print_(f'Scrapping from page {scrape_range[0]} to page {scrape_range[1] - 1}')

    # create jobs with window size
    for _ in range(*scrape_range):
        await ad_list_jobs_q.put(_)

    # create elegant loading animation in new thread :))
    t = threading.Thread(target=animate)
    t.start()

    while CREATE_TASK or ad_list_res_q.qsize() != 0:

        response = await ad_list_res_q.get()
        _res, page = response
        # convert to json
        _res = _res.json()

        MESSAGE = 'got response for page ' + str(page)

        # check if it is the last page of API
        async with LOCK:
            is_last_page = _is_last_page(_res)

        # update some values... code tells the jobs is done here :\
        if not is_last_page:
            async with LOCK:
                _last_scraped_page = page if page > _last_scraped_page else _last_scraped_page

        if not is_last_page and _last_page_to_be_scraped <= THRESHOLD:
            async with LOCK:
                _last_page_to_be_scraped += 1

            # create new job (scrape new page)
            if CREATE_TASK and _last_page_to_be_scraped <= THRESHOLD:
                await ad_list_jobs_q.put(_last_page_to_be_scraped)

            # create new job to get the ad detail later (after this process is done)
            for ad_id in _get_ads_ids(_res):
                await ad_detail_jobs_q.put(ad_id)

        # when last page seen don't create new job
        elif _last_page_to_be_scraped - 1 == _last_scraped_page or is_last_page:
            async with LOCK:
                CREATE_TASK = False
                await ad_list_jobs_q.join()

    DONE = True
    t.join()

    print_('last page is ' + str(_last_scraped_page))
    if not is_last_page:
        print_(f'threshold {THRESHOLD} page reached.')

    # cancel all tasks
    [task.cancel() for task in ad_list_tasks]
    await gather(*ad_list_tasks, return_exceptions=True)


async def main():
    load_config()
    # create some Queues to pass values between workers
    ad_list_jobs_q = Queue()
    ad_list_res_q = Queue()
    ad_detail_jobs_q = Queue()
    ad_detail_res_q = Queue()

    qs = ((ad_list_jobs_q, ad_list_res_q), (ad_detail_jobs_q, ad_detail_res_q))

    # create a new async session with coockies values
    asession = AsyncHTMLSession()
    asession.cookies.update(COOKIES)

    start = time.monotonic()
    # do the actual process
    await get_ads_list(qs, asession)
    await get_ads_detail(qs, asession)
    end = time.monotonic()

    print_(f'{round(end - start, 2)} seconds elapsed.')


asyncio.run(main(), debug=False)

print_('saving dataframe to file.')
# save values to dataframe
df = pd.DataFrame(list_of_dicts)

try:
    # if there was a csv file present load it
    old_df = pd.read_csv('data.csv')
    # concat two dataframes
    df = df.append(old_df, ignore_index=True)
    # drop duplicate 'ad_id's
    # this means in the presence of old data this script
    # do not remove old values that are not present now on the website
    # and only ad new records to the data.csv
    df.drop_duplicates(subset='ad_id', ignore_index=True, inplace=True)
except Exception:
    pass

scraped_timestamp = jdatetime.datetime.now().strftime(DATETIME_FORMAT)
NEIGHBORHOOD = str(NEIGHBORHOOD)
# write to csv file
df.to_csv(f'data_{NEIGHBORHOOD}_{scraped_timestamp}.csv', index=False)
# write to excel file
df.to_excel(f'data_{NEIGHBORHOOD}_{scraped_timestamp}.xlsx')
