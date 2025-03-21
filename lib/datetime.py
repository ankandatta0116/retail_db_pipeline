import datetime

def get_current_date():
    return datetime.datetime.today().strftime("%Y-%m-%d")

def get_current_time():
    return datetime.datetime.today().strftime("%H_%M_%S")