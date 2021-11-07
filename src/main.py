import time
import multiprocessing as mp
import pandas as pd
from pandas.errors import ParserError
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

def on_created(event):
    """ Called when new CSV added to directory

    When a new CSV file is added to the data directory, the observer thread triggers
    the event handler to call this method. Here each task for each new CSV file created
    is assigned a subprocess in the multiprocessing pool, this allows for multiple CSV files
    to be processed in parallel.

    Args:
        event: The FileCreated event object 
    """
    try:
        pool.apply_async(process_CSV, (event.src_path,))
    except Exception as e:
        print(e)

def process_CSV(src_path):
    """Processes each CSV file and generates new ones

    The CSV file just added to the directory is read into a pandas dataframe then only select
    columns are copied over to a new dataframe. These columns are the renamed according to the
    format specified in the requirements. A new column (roas) is then created based on the formula
    conversion value / cost. Finally, this new dataframe is written to a new CSV file with the
    “processed/$currency/search_terms/$timestamp.csv” format

    Args:
        src_path: The source path of the CSV file added
    """
    try:
        # Reads the data from the added CSV into a dataframe
        old_data= pd.read_csv(
            src_path,
            sep = '\t',
            encoding='UTF-16 LE',
            thousands=',',
            dtype={"Search term": str, "clicks": int, "Ad group": str, "Conv. value": float},
            error_bad_lines=False,
            warn_bad_lines=True)

        # Creates a new dataframe based on the old one
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
            inplace=True)

        # Adds new calculated roas column to dataframe
        new_data['roas'] =  new_data['conversion_value'] / new_data['cost']
        new_data.to_csv(
            f"../processed/{old_data.iloc[0]['Currency code']}/search_terms/{str(time.time())}.csv", 
            index=False)
    except ParserError:
        print("Your data contained rows that could not be parsed.")

if __name__ == "__main__":
    # Multiprocessing Pool settings
    pool = mp.Pool(processes=mp.cpu_count())

    # Settings for the event handler
    patterns = ["*.csv"]
    ignore_patterns = None
    ignore_directories = False
    case_sensitive = True
    my_event_handler = PatternMatchingEventHandler(patterns, ignore_patterns, ignore_directories, case_sensitive)
    my_event_handler.on_created = on_created

    # Settings for the observer thread that monitors the directory for changes
    path = "../data"
    my_observer = Observer()
    my_observer.schedule(my_event_handler, path, recursive=False)
    my_observer.start()

    # Starts the observer thread
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        my_observer.stop()

    my_observer.join()
    pool.close()
    pool.join()
