import httpx
from src.py_load_eurostat.fetcher import Fetcher
from src.py_load_eurostat.config import Settings


def main():
    """
    Example script to demonstrate the Fetcher functionality.
    """
    print("Initializing settings and fetcher...")
    settings = Settings()

    # In a real application, you would manage the client's lifecycle,
    # for example, using a context manager.
    with httpx.Client(
        headers={"User-Agent": "py-load-eurostat/0.1.0"},
        follow_redirects=True,
        timeout=60.0,
    ) as client:
        fetcher = Fetcher(settings, client=client)

        # --- List available datasets ---
        print("\nFetching list of available datasets from Eurostat...")
        available_datasets = fetcher.list_available_datasets()
        if available_datasets:
            print(f"Successfully fetched {len(available_datasets)} total datasets.")
            print("Here are the first 5:")
            for dataset in available_datasets[:5]:
                print(f"  - Code: {dataset['code']}, Title: {dataset['title']}")
        else:
            print("Failed to fetch the list of datasets.")
            return

        # --- Download a specific dataset ---
        dataset_to_download = "nama_10_gdp"
        print(f"\nAttempting to download the '{dataset_to_download}' dataset...")

        download_path = fetcher.download_dataset(dataset_to_download)

        if download_path and download_path.exists():
            print(
                f"\nSuccessfully downloaded '{dataset_to_download}' to '{download_path}'"
            )
            file_size = download_path.stat().st_size
            print(f"File size: {file_size} bytes.")
        else:
            print(f"\nFailed to download the '{dataset_to_download}' dataset.")


if __name__ == "__main__":
    main()
