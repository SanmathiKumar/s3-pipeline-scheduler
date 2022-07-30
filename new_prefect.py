from prefect import task

@task
def extract():
    # download and return a list of all texas realtors
    ...
    return all_data

@task
def transform()
    # filter list to houston realtors
    ...
    return filtered_data

@task
def load():
    # store data into database
    ...
    pass