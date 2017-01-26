# Level is an int 0 through 2, where 0 is quiet, 1 is errors, and 2 is verbose
def log(my_level, assigned_level, message):
    if (my_level >= assigned_level):
        print(message)
