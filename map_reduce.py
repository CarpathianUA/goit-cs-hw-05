import string
import requests
import os
import matplotlib.pyplot as plt

from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict


def get_text(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.RequestException:
        return None


def remove_punctuation(text: str):
    return text.translate(str.maketrans("", "", string.punctuation))


def map_function(word):
    return word, 1


def shuffle_function(mapped_values):
    shuffled = defaultdict(list)
    for key, value in mapped_values:
        shuffled[key].append(value)
    return shuffled.items()


def reduce_function(key_values):
    key, values = key_values
    return key, sum(values)


# Виконання MapReduce
def map_reduce(text, search_words=None):
    text = remove_punctuation(text)
    words = text.split()

    if search_words:
        words = [word for word in words if word in search_words]

    with ThreadPoolExecutor(max_workers=os.cpu_count() * 2) as executor:
        mapped_values = list(executor.map(map_function, words))

    shuffled_values = shuffle_function(mapped_values)

    with ThreadPoolExecutor(max_workers=os.cpu_count() * 2) as executor:
        reduced_values = list(executor.map(reduce_function, shuffled_values))

    return dict(reduced_values)


def top_ten_words(result):
    return dict(sorted(result.items(), key=lambda x: x[1], reverse=True)[:10])


def visualize_top_words(result):
    plt.title("Top ten words in 1984 by George Orwell")
    plt.xlabel("Word")
    plt.ylabel("Count")
    plt.bar(top_ten_words(result).keys(), top_ten_words(result).values())  # noqa
    plt.show()


if __name__ == "__main__":
    url = "https://gutenberg.net.au/ebooks01/0100021.txt"  # 1984
    text = get_text(url)
    try:
        result = map_reduce(text)
        visualize_top_words(result)
    except Exception as e:
        print(e)
