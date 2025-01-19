import string
import requests
import matplotlib.pyplot as plt

from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict


# function to fetch text from a URL
def get_text(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"Error fetching text from {url}: {e}")
        return None


# function for removing punctuation
def remove_punctuation(text):
    translator = str.maketrans("", "", string.punctuation)
    return text.translate(translator)


# function for mapping
def map_function(word):
    return word, 1


def shuffle_function(mapped_values):
    shuffled = defaultdict(list)
    for key, value in mapped_values:
        shuffled[key].append(value)
    return shuffled.items()


def reduce_function(key_values):
    key, value = key_values
    return key, sum(value)


def map_reduce(text, num_top_words=0):
    # Remove punctuation
    cleaned_text = remove_punctuation(text)

    # Split text into words
    words = cleaned_text.split()

    # Step 1: parallel mapping
    with ThreadPoolExecutor() as executor:
        mapped_values = list(executor.map(map_function, words))

    # Step 2: Shuffling
    shuffled_values = shuffle_function(mapped_values)

    # Step 3: parallel reduction
    with ThreadPoolExecutor() as executor:
        reduced_values = list(executor.map(reduce_function, shuffled_values))

    # Sort reduced values
    sorted_values = sorted(reduced_values, key=lambda x: x[1], reverse=True)

    return (
        dict(sorted_values[:num_top_words])
        if num_top_words > 0
        else dict(sorted_values)
    )

def visualise_top_words(top_words):
    labels = list(top_words.keys())
    values = list(top_words.values())

    plt.figure(figsize=(10, 6))
    bars = plt.barh(labels, values)
    plt.xlabel("Frequency")
    plt.ylabel("Words")
    plt.title("Top 10 Words Count")
    # Adding the value text to each bar
    for bar in bars:
        width = bar.get_width()
        plt.text(
            width + 0.5,  # Position slightly to the right of the bar
            bar.get_y()
            + bar.get_height() / 2,  # Vertical position at the center of the bar
            f"{width:.0f}",  # Format the text (remove decimals if integer)
            va="center",  # Align text vertically to the center
        )
    plt.tight_layout()
    plt.gca().invert_yaxis()
    plt.show()


if __name__ == "__main__":
    # book for processing: Pride and Prejudice 
    book_url = "https://www.gutenberg.org/files/1342/1342-0.txt"
    book_text = get_text(book_url)

    if book_text:
        # Get top 10 words
        top_words = map_reduce(book_text, num_top_words=10)
        print("Top 10 words count:", top_words)
        visualise_top_words(top_words)
    else:
        print("Error fetching book text.")
