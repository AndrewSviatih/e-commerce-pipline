import sys
import requests


def download_file_from_google_drive(file_id, destination):
    URL = "https://drive.google.com/drive/folders/1YjWgELyLoR1zy7I4HO5ewV6VP0EXGeAP?usp=sharing"

    session = requests.Session()

    response = session.get(URL, params={"id": file_id}, stream=True)
    token = get_confirm_token(response)

    if token:
        params = {"id": file_id, "confirm": token}
        response = session.get(URL, params=params, stream=True)

    save_response_content(response, destination)


def get_confirm_token(response):
    for key, value in response.cookies.items():
        if key.startswith("download_warning"):
            return value

    return None


def save_response_content(response, destination):
    CHUNK_SIZE = 32768

    with open(destination, "wb") as f:
        for chunk in response.iter_content(CHUNK_SIZE):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)


def main():
    if len(sys.argv) >= 3:
        file_id = sys.argv[1]
        destination = sys.argv[2]
    else:
        file_id = "1YjWgELyLoR1zy7I4HO5ewV6VP0EXGeAP"
        destination = "./jars/"
    print(f"dowload {file_id} to {destination}")
    download_file_from_google_drive(file_id, destination)


if __name__ == "__main__":
    main()