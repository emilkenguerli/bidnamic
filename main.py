import time
import multiprocessing as mp
import pandas as pd
from pandas.errors import ParserError
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

def on_created(event):
    try:
        pool.apply_async(createCSV, (event.src_path,))
    except Exception as e:
        print(e)

def createCSV(src_path):
    try:
        old_data= pd.read_csv(
            src_path,
            sep = '\t',
            encoding='UTF-16 LE',
            thousands=',',
            dtype={"Search term": str, "clicks": int, "Ad group": str, "Conv. value": float},
            error_bad_lines=False,
            warn_bad_lines=True
        )
        new_data = old_data[['Search term', 'Clicks', 'Cost', 'Impr.', 'Conv. value']].copy()
        new_data.rename(
            {
                'Search term': 'search_term', 
                'Clicks': 'clicks', 
                'Cost': 'cost', 
                'Impr.': 'impressions', 
                'Conv. value': 'conversion_value'
            }
            , axis=1, 
            inplace=True
        )
        new_data['roas'] =  new_data['conversion_value'] / new_data['cost']
        new_data.to_csv(
            f"processed/{old_data.iloc[0]['Currency code']}/search_terms/{str(time.time())}.csv", 
            index=False
        )
    except ParserError:
        print("Your data contained rows that could not be parsed.")

if __name__ == "__main__":
    pool = mp.Pool(processes=mp.cpu_count())
    patterns = ["*.csv"]
    ignore_patterns = None
    ignore_directories = False
    case_sensitive = True
    my_event_handler = PatternMatchingEventHandler(patterns, ignore_patterns, ignore_directories, case_sensitive)

    my_event_handler.on_created = on_created

    path = "."
    go_recursively = False
    my_observer = Observer()
    my_observer.schedule(my_event_handler, path, recursive=go_recursively)

    my_observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        my_observer.stop()
    my_observer.join()
    pool.close()
    pool.join()
