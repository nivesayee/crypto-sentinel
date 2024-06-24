import requests
import csv
from integration.api.config import coin_gecko_api_url, crypto_list_file


def fetch_crypto_list(url):
    """
    Fetch list of cryptocurrencies from the given URL

    Parameters:
        url (str): URL from where the cryptocurrency list is to be fetched

    Returns:
        crypto_curr_list (list): List of tuples that hold the cryptocurrencies information like id, symbol and name
    """
    response = requests.get(url)
    data = response.json()
    crypto_curr_list = [(coin['id'], coin['symbol'], coin['name']) for coin in data]
    return crypto_curr_list


def save_crypto_list(crypto_curr_list, file_path):
    """
    Saves the cryptocurrency list to a csv file

    Parameters:
        crypto_curr_list (list): List of tuples that hold the cryptocurrencies information like id, symbol and name
        file_path (str): Path of the file to be written

    Returns:
        None
    """
    header_names = ["id", "symbol", "name"]
    with open(file_path, 'w', encoding='utf-8', newline="") as crypto_file:
        writer = csv.writer(crypto_file)
        writer.writerow(header_names)
        writer.writerows(crypto_curr_list)


if __name__ == '__main__':
    crypto_list = fetch_crypto_list(coin_gecko_api_url)
    try:
        save_crypto_list(crypto_list, crypto_list_file)
        print("Crypto list saved to", crypto_list_file)
    except Exception as e:
        print("Error:", e, "while trying to save crypto_list to", crypto_list_file)
