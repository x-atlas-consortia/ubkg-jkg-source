# Utilities to print in color to the terminal

def print_color(colorcode:str, message:str) -> str:
    """
    Prints a colored message
    :param colorcode: ASCII color code
    :param message: message to print
    """
    if colorcode == 'red':
        asciicode = 31
    elif colorcode == 'green':
        asciicode = 32
    elif colorcode == 'yellow':
        asciicode = 33
    elif colorcode == 'blue':
        asciicode = 34
    elif colorcode == 'magenta':
        asciicode = 35
    elif colorcode == 'cyan':
        asciicode = 36
    else: # default = white
        asciicode = 37

    print(f"\033[{asciicode}m{message}\033[0m")