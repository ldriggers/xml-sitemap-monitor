# Competitor Sitemap Monitor

This project is a lightweight business intelligence system designed to track changes to competitors' websites by monitoring their XML sitemaps. It automatically fetches sitemap data, identifies new, updated, and removed URLs, and maintains a historical record of these changes.

## Overview

The system focuses on:
- Monitoring XML sitemaps from configured competitor domains.
- Detecting new, updated (based on `lastmod` dates), and removed URLs.
- Storing URL metadata and change history in Parquet files.
- Running daily via GitHub Actions for automated, cost-effective monitoring.

This initial version does **not** store the actual content of the web pages, only sitemap metadata.

## Project Structure

```
/
├── .github/
│   └── workflows/
│       └── daily_monitor.yml  # GitHub Actions workflow
├── competitive_content_monitoring/
│   ├── src/                     # Source code
│   │   ├── __init__.py
│   │   ├── config.py            # Handles loading and validating config.json
│   │   ├── sitemap_fetcher.py   # Fetches sitemaps
│   │   ├── sitemap_parser.py    # Parses sitemap XML
│   │   ├── data_processor.py    # Processes changes and updates data for each domain
│   │   └── main.py              # Main script orchestrating the tasks
│   ├── data/                    # Stores data files (created by the script)
│   │   └── .gitkeep
│   ├── requirements.txt         # Python dependencies
│   ├── config.json              # Configuration for domains to monitor
│   ├── PLAN.md                  # Project plan document
│   └── prd.md                   # Product requirements document
└── README.md                  # This file
```
(Note: The main project directory `competitive_content_monitoring` might be the root in your local setup if you cloned it directly, or a subdirectory if it's part of a larger workspace.)

## Setup

1.  **Clone the repository.**
2.  **Configure Domains**: Edit `competitive_content_monitoring/config.json` to specify the domains you want to monitor. Update the `user_agent` field to something relevant to your project (including a contact URL or email is good practice).
    ```json
    {
      "domains": [
        {
          "name": "Example Competitor 1",
          "domain": "example.com",
          "sitemap_url": "https://www.example.com/sitemap.xml"
        },
        {
          "name": "Example Competitor 2",
          "domain": "another-example.net",
          "sitemap_url": "https://www.another-example.net/sitemap_index.xml"
        }
      ],
      "user_agent": "YourProjectSitemapMonitor/1.0 (+http://your-contact-page-or-email)"
    }
    ```
3.  **Install Dependencies**: 
    ```bash
    pip install -r competitive_content_monitoring/requirements.txt
    ```

## Usage

-   **Manual Run (for testing)**: Navigate to the `competitive_content_monitoring` project root directory and run the main script:
    ```bash
    python src/main.py
    ```
    (Ensure your current working directory is the project root, `competitive_content_monitoring`, not the `src` directory, for `config.json` to be found correctly by default).
-   **Automated Runs**: The system is configured to run daily via GitHub Actions. Changes to the `data/` directory (containing Parquet, CSV, and JSON files) will be committed back to the repository by the action.

## Data Output

The script processes each domain specified in `config.json` and generates/updates data files in the `competitive_content_monitoring/data/` directory. For each domain (e.g., `example.com`), the following files are created:

-   `example.com_urls.parquet`: Contains all unique URLs found for `example.com`, with their metadata (like `loc`, `lastmod`), detection timestamps, and a `change_type` column indicating if the URL is 'new', 'updated', 'removed', or 'unchanged' in the latest run. This is the primary data file for analysis.
-   `example.com_urls.csv`: A CSV version of the Parquet file for easier ad-hoc viewing or use in tools that prefer CSV.
-   `example.com_urls.json`: A JSON Lines version of the data, where each line is a JSON object representing a URL entry.

These files store the history and current state of all discovered page URLs from the sitemaps. The `DataProcessor` module handles loading previous data, comparing it with the latest fetch, and identifying changes.

## Contributing

(Details to be added if the project becomes open to contributions)

## License

(To be determined - e.g., MIT, Apache 2.0, or private) 