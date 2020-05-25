from colorama import init, Fore
import re

init()


def colorify(color: str, text: str) -> str:
    """This function colorized the foreground of a string.
    @param {string} color - The color to use.
    @param {string} text - The text to colorize.
    @return {string} - The colorized string."""
    return color + text + Fore.RESET


def sanitize_string(text: str) -> str:
    """This function removes all non word characters and _ character from a string.
    @param {string} text - The text to sanitize.
    @return {string} - The sanitized text."""
    return re.sub(r"[^a-zA-Z0-9]", "", text)
