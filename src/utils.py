from colorama import init, Fore

init()


def colorify(color, text):
    return color + text + Fore.RESET
